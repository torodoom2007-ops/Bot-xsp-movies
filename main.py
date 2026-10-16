import os
import re
import html
import logging
import asyncio
from difflib import SequenceMatcher
from contextlib import asynccontextmanager

import asyncpg
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import PlainTextResponse
from aiogram import Bot, Dispatcher, F
from aiogram.types import (
    Update, Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton,
    BotCommand, CopyMessage
)
from aiogram.enums import ParseMode
from aiogram.client.default import DefaultBotProperties
from aiogram.exceptions import TelegramAPIError, TelegramForbiddenError, TelegramBadRequest

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
log = logging.getLogger("movie-time")

BOT_TOKEN = os.environ["BOT_TOKEN"].strip()
ADMIN_ID = int(os.getenv("ADMIN_ID", "8017590244"))
GROUP_ID = int(os.getenv("GROUP_ID", "-1004309235247"))
BOT_USERNAME = os.getenv("BOT_USERNAME", "Ms_blon_bot").lstrip("@")
BRAND_URL = os.getenv("BRAND_URL", "https://t.me/movie_time_by")
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "").strip()
PUBLIC_URL = os.getenv("RENDER_EXTERNAL_URL", "").rstrip("/")
MAX_RESULTS = 8
MAX_BATCH = 50

if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL is required. Use a persistent PostgreSQL provider.")

# Render/Supabase commonly provide postgres:// or postgres://...; asyncpg accepts postgresql://.
DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

bot = Bot(BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher()
pool: asyncpg.Pool | None = None


def esc(value: object) -> str:
    return html.escape(str(value or ""), quote=True)


def clean_title(raw: str) -> str:
    """Keep only a human-entered title; remove common labels and surrounding decoration."""
    value = (raw or "").strip()
    value = re.sub(r"^(?:שם הסרט|שם|title|movie)\s*[:\-–]\s*", "", value, flags=re.I)
    value = re.sub(r"\s+", " ", value)
    return value[:250].strip(" \t\r\n|•—-")


def normalize(value: str) -> str:
    value = (value or "").casefold()
    value = re.sub(r"[^\w\u0590-\u05ff]+", " ", value, flags=re.UNICODE)
    return re.sub(r"\s+", " ", value).strip()


def is_admin(user_id: int) -> bool:
    return user_id == ADMIN_ID


def brand_footer() -> str:
    return f'\n\n🎬 <a href="{BRAND_URL}">Movie Time</a>'


def main_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⌕ חיפוש סרטים", callback_data="search_help")],
        [InlineKeyboardButton(text="✦ Movie Time", url=BRAND_URL)]
    ])


async def init_db():
    global pool
    pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=5, command_timeout=30)
    async with pool.acquire() as con:
        await con.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id BIGINT PRIMARY KEY,
            username TEXT,
            first_name TEXT,
            last_seen TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            blocked BOOLEAN NOT NULL DEFAULT FALSE
        );
        CREATE TABLE IF NOT EXISTS movies (
            id BIGSERIAL PRIMARY KEY,
            title TEXT NOT NULL,
            title_norm TEXT NOT NULL,
            file_id TEXT NOT NULL,
            file_unique_id TEXT,
            media_type TEXT NOT NULL,
            source_chat_id BIGINT,
            source_message_id BIGINT,
            added_by BIGINT NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            active BOOLEAN NOT NULL DEFAULT TRUE
        );
        CREATE INDEX IF NOT EXISTS movies_title_norm_idx ON movies (title_norm);
        CREATE INDEX IF NOT EXISTS movies_created_idx ON movies (created_at DESC);
        CREATE TABLE IF NOT EXISTS ingest_queue (
            id BIGSERIAL PRIMARY KEY,
            admin_id BIGINT NOT NULL,
            file_id TEXT NOT NULL,
            file_unique_id TEXT,
            media_type TEXT NOT NULL,
            source_chat_id BIGINT NOT NULL,
            source_message_id BIGINT NOT NULL,
            status TEXT NOT NULL DEFAULT 'queued',
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        CREATE INDEX IF NOT EXISTS ingest_queue_status_idx ON ingest_queue (admin_id, status, id);
        CREATE TABLE IF NOT EXISTS admin_state (
            admin_id BIGINT PRIMARY KEY,
            state TEXT,
            data JSONB NOT NULL DEFAULT '{}'::jsonb,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        CREATE TABLE IF NOT EXISTS search_logs (
            id BIGSERIAL PRIMARY KEY,
            user_id BIGINT,
            query TEXT,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        """)
    log.info("PostgreSQL schema ready")


async def register_user(user):
    if not user:
        return
    async with pool.acquire() as con:
        await con.execute("""
        INSERT INTO users(user_id, username, first_name, last_seen, blocked)
        VALUES($1,$2,$3,NOW(),FALSE)
        ON CONFLICT(user_id) DO UPDATE SET username=EXCLUDED.username,
        first_name=EXCLUDED.first_name, last_seen=NOW()
        """, user.id, user.username, user.first_name)


async def get_state(admin_id: int):
    async with pool.acquire() as con:
        row = await con.fetchrow("SELECT state,data FROM admin_state WHERE admin_id=$1", admin_id)
        return (row["state"], dict(row["data"] or {})) if row else (None, {})


async def set_state(admin_id: int, state: str | None, data: dict | None = None):
    async with pool.acquire() as con:
        await con.execute("""
        INSERT INTO admin_state(admin_id,state,data,updated_at) VALUES($1,$2,$3::jsonb,NOW())
        ON CONFLICT(admin_id) DO UPDATE SET state=EXCLUDED.state,data=EXCLUDED.data,updated_at=NOW()
        """, admin_id, state, json_dumps(data or {}))


def json_dumps(value):
    import json
    return json.dumps(value, ensure_ascii=False)


def media_details(message: Message):
    if message.video:
        return message.video.file_id, message.video.file_unique_id, "video"
    if message.document:
        return message.document.file_id, message.document.file_unique_id, "document"
    if message.animation:
        return message.animation.file_id, message.animation.file_unique_id, "animation"
    if message.audio:
        return message.audio.file_id, message.audio.file_unique_id, "audio"
    if message.video_note:
        return message.video_note.file_id, message.video_note.file_unique_id, "video_note"
    if message.photo:
        photo = message.photo[-1]
        return photo.file_id, photo.file_unique_id, "photo"
    return None


async def send_media(chat_id: int, file_id: str, media_type: str, caption: str | None = None):
    caption = caption or None
    if media_type == "video":
        return await bot.send_video(chat_id, file_id, caption=caption, supports_streaming=True)
    if media_type == "document":
        return await bot.send_document(chat_id, file_id, caption=caption)
    if media_type == "animation":
        return await bot.send_animation(chat_id, file_id, caption=caption)
    if media_type == "audio":
        return await bot.send_audio(chat_id, file_id, caption=caption)
    if media_type == "video_note":
        return await bot.send_video_note(chat_id, file_id)
    if media_type == "photo":
        return await bot.send_photo(chat_id, file_id, caption=caption)
    raise ValueError("Unsupported media type")


async def process_next_title(admin_id: int):
    async with pool.acquire() as con:
        row = await con.fetchrow("""
            SELECT * FROM ingest_queue
            WHERE admin_id=$1 AND status='queued'
            ORDER BY id ASC LIMIT 1
        """, admin_id)
        if not row:
            await set_state(admin_id, None, {})
            await bot.send_message(admin_id,
                "╭━━━ ✦ MOVIE TIME ✦ ━━━╮\n"
                "✓ התור הסתיים בהצלחה\n"
                "כל הקבצים שהוזנו נשמרו במאגר.\n"
                "╰━━━━━━━━━━━━━━━━━━╯" + brand_footer())
            return
        await con.execute("UPDATE ingest_queue SET status='naming' WHERE id=$1", row["id"])
    await set_state(admin_id, "await_title", {"queue_id": row["id"]})
    # Echo the exact original Telegram message to the admin, preserving caption/visible details.
    try:
        await bot.copy_message(admin_id, row["source_chat_id"], row["source_message_id"])
    except TelegramAPIError:
        await send_media(admin_id, row["file_id"], row["media_type"])
    await bot.send_message(
        admin_id,
        f"╭━━━ ✦ זיהוי סרט ✦ ━━━╮\n"
        f"פריט בתור: <b>#{row['id']}</b>\n"
        "מה שם הסרט?\n\n"
        "שלח את השם בלבד. כל טקסט אחר לא יישמר כשם הסרט.\n"
        "╰━━━━━━━━━━━━━━━━━━╯" + brand_footer(),
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⏭ דלג על הקובץ", callback_data=f"skip:{row['id']}")],
            [InlineKeyboardButton(text="✕ ביטול מצב קליטה", callback_data="cancel_ingest")]
        ])
    )


async def search_movies(query: str):
    q = normalize(query)
    if not q:
        return []
    async with pool.acquire() as con:
        rows = await con.fetch("""
            SELECT id,title,title_norm,file_id,media_type
            FROM movies WHERE active=TRUE
            ORDER BY CASE WHEN title_norm=$1 THEN 0 WHEN title_norm LIKE $2 THEN 1
                          WHEN title_norm LIKE $3 THEN 2 ELSE 3 END,
                     created_at DESC
            LIMIT 150
        """, q, q + "%", "%" + q + "%")
    scored = []
    for row in rows:
        title_norm = row["title_norm"]
        if q in title_norm:
            score = 1.0 if title_norm == q else (0.93 if title_norm.startswith(q) else 0.84)
        else:
            score = SequenceMatcher(None, q, title_norm).ratio()
            # Token overlap helps multiword titles and Hebrew/English partial matches.
            qt, tt = set(q.split()), set(title_norm.split())
            if qt and tt:
                score = max(score, len(qt & tt) / len(qt | tt) * 0.85)
        if score >= 0.42:
            scored.append((score, row))
    scored.sort(key=lambda item: (-item[0], item[1]["title_norm"]))
    return [row for _, row in scored[:MAX_RESULTS]]


def results_keyboard(rows):
    buttons = []
    for row in rows:
        title = row["title"]
        if len(title) > 52:
            title = title[:49] + "…"
        buttons.append([InlineKeyboardButton(text=f"▶ {title}", callback_data=f"movie:{row['id']}")])
    buttons.append([InlineKeyboardButton(text="⌕ חיפוש נוסף", switch_inline_query_current_chat="")])
    buttons.append([InlineKeyboardButton(text="✦ Movie Time", url=BRAND_URL)])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


async def reply_search(message: Message, query: str):
    query = query.strip()
    if len(query) < 2:
        await message.reply("⌕ כתבו לפחות שתי אותיות כדי לחפש במאגר.")
        return
    async with pool.acquire() as con:
        await con.execute("INSERT INTO search_logs(user_id,query) VALUES($1,$2)", message.from_user.id, query[:250])
    rows = await search_movies(query)
    if not rows:
        await message.reply(
            "╭━━━ ✦ MOVIE TIME ✦ ━━━╮\n"
            f"לא נמצאו תוצאות עבור <b>{esc(query[:100])}</b>.\n"
            "נסו שם מלא או חלק מהשם.\n"
            "╰━━━━━━━━━━━━━━━━━━╯" + brand_footer()
        )
        return
    lines = [
        "╭━━━ ✦ MOVIE TIME ✦ ━━━╮",
        f"נמצאו <b>{len(rows)}</b> תוצאות עבור <b>{esc(query[:100])}</b>.",
        "בחרו סרט כדי לקבל אותו בהודעה פרטית:",
        "╰━━━━━━━━━━━━━━━━━━╯"
    ]
    await message.reply("\n".join(lines) + brand_footer(), reply_markup=results_keyboard(rows))


@dp.message(F.text == "/start")
async def start(message: Message):
    await register_user(message.from_user)
    parts = (message.text or "").split(maxsplit=1)
    if len(parts) == 2 and parts[1].startswith("movie_"):
        try:
            movie_id = int(parts[1].split("_", 1)[1])
            async with pool.acquire() as con:
                row = await con.fetchrow("SELECT * FROM movies WHERE id=$1 AND active=TRUE", movie_id)
            if row:
                caption = f"╭━━━ ✦ MOVIE TIME ✦ ━━━╮\\n🎬 <b>{esc(row['title'])}</b>\\n╰━━━━━━━━━━━━━━━━━━╯" + brand_footer()
                await send_media(message.chat.id, row["file_id"], row["media_type"], caption)
                return
        except (ValueError, TelegramAPIError):
            pass
    await message.answer(
        "╭━━━━━━━━━━━━━━━━━━╮\n"
        "       <b>MOVIE TIME</b>\n"
        "╰━━━━━━━━━━━━━━━━━━╯\n\n"
        "ברוכים הבאים למאגר הסרטים.\n"
        "⌕ חפשו סרט בקבוצה או כתבו כאן את שמו.\n\n"
        "בחרו חיפוש או התחילו לגלות:" + brand_footer(),
        reply_markup=main_menu()
    )


@dp.message(F.text.startswith("/admin"))
async def admin_panel(message: Message):
    if not is_admin(message.from_user.id):
        return
    async with pool.acquire() as con:
        movies = await con.fetchval("SELECT COUNT(*) FROM movies WHERE active=TRUE")
        users = await con.fetchval("SELECT COUNT(*) FROM users WHERE blocked=FALSE")
        queued = await con.fetchval("SELECT COUNT(*) FROM ingest_queue WHERE status='queued'")
    await message.answer(
        "╭━━━ ✦ MOVIE TIME ADMIN ✦ ━━━╮\n\n"
        f"🎞 סרטים במאגר: <b>{movies:,}</b>\n"
        f"👥 משתמשים: <b>{users:,}</b>\n"
        f"▤ קבצים שממתינים בתור: <b>{queued:,}</b>\n\n"
        "בחר פעולה:" + brand_footer(),
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="＋ התחלת קליטה", callback_data="ingest_start")],
            [InlineKeyboardButton(text="▤ מצב התור", callback_data="queue_status")],
            [InlineKeyboardButton(text="📊 סטטיסטיקה", callback_data="stats")],
            [InlineKeyboardButton(text="📢 שידור למשתמשים", callback_data="broadcast_start")],
            [InlineKeyboardButton(text="📣 הודעה לקבוצה", callback_data="group_broadcast_start")]
        ])
    )


@dp.callback_query(F.data == "ingest_start")
async def ingest_start(call: CallbackQuery):
    if not is_admin(call.from_user.id):
        await call.answer("אין הרשאה", show_alert=True); return
    await set_state(ADMIN_ID, "ingest_upload", {})
    await call.message.answer(
        "╭━━━ ✦ קליטת סרטים ✦ ━━━╮\n"
        "שלח עכשיו עד 50 קבצי מדיה, אחד אחרי השני.\n"
        "אפשר לשלוח וידאו, מסמך או תמונה.\n"
        "בסיום שלח /finish כדי להתחיל לזהות את שמות הסרטים.\n"
        "╰━━━━━━━━━━━━━━━━━━╯" + brand_footer()
    )
    await call.answer()


@dp.message(F.text == "/finish")
async def finish_upload(message: Message):
    if not is_admin(message.from_user.id):
        return
    state, _ = await get_state(ADMIN_ID)
    if state != "ingest_upload":
        await message.answer("אין סבב קליטה פעיל. פתחו אותו דרך /admin.")
        return
    await set_state(ADMIN_ID, None, {})
    await message.answer("✓ הקליטה הסתיימה. מתחיל תהליך זיהוי לפי סדר הקבצים.")
    await process_next_title(ADMIN_ID)


@dp.message(F.text == "/cancel")
async def cancel(message: Message):
    if not is_admin(message.from_user.id):
        return
    await set_state(ADMIN_ID, None, {})
    await message.answer("✕ הפעולה הנוכחית בוטלה. קבצים שכבר נשמרו לא נמחקו.")


@dp.message(F.text == "/broadcast")
async def broadcast_start(message: Message):
    if not is_admin(message.from_user.id):
        return
    await set_state(ADMIN_ID, "broadcast_private", {})
    await message.answer("שלח את ההודעה לשידור לכל המשתמשים שהפעילו את הבוט. /cancel לביטול.")


@dp.message(F.text == "/groupbroadcast")
async def group_broadcast_start(message: Message):
    if not is_admin(message.from_user.id):
        return
    await set_state(ADMIN_ID, "broadcast_group", {})
    await message.answer("שלח את ההודעה לפרסום בקבוצה המוגדרת. /cancel לביטול.")


@dp.message(F.text)
async def text_router(message: Message):
    if not message.from_user:
        return
    await register_user(message.from_user)
    text = (message.text or "").strip()
    if is_admin(message.from_user.id):
        state, data = await get_state(ADMIN_ID)
        if state == "await_title":
            title = clean_title(text)
            if not title or title.startswith("/"):
                await message.answer("שלח שם סרט תקין בלבד, או לחץ דלג.")
                return
            queue_id = int(data["queue_id"])
            async with pool.acquire() as con:
                row = await con.fetchrow("SELECT * FROM ingest_queue WHERE id=$1 AND status='naming'", queue_id)
                if not row:
                    await set_state(ADMIN_ID, None, {})
                    await message.answer("הפריט בתור לא נמצא. פתח /admin ובדוק את התור.")
                    return
                await con.execute("""
                    INSERT INTO movies(title,title_norm,file_id,file_unique_id,media_type,
                    source_chat_id,source_message_id,added_by)
                    VALUES($1,$2,$3,$4,$5,$6,$7,$8)
                """, title, normalize(title), row["file_id"], row["file_unique_id"],
                     row["media_type"], row["source_chat_id"], row["source_message_id"], ADMIN_ID)
                await con.execute("UPDATE ingest_queue SET status='saved' WHERE id=$1", queue_id)
            await message.answer(
                "╭━━━ ✓ נשמר במאגר ━━━╮\n"
                f"🎬 <b>{esc(title)}</b>\n"
                "הסרט נשמר בהצלחה.\n"
                "╰━━━━━━━━━━━━━━━━━━╯" + brand_footer()
            )
            await process_next_title(ADMIN_ID)
            return
        if state == "broadcast_private":
            await set_state(ADMIN_ID, None, {})
            await do_broadcast(message, "private")
            return
        if state == "broadcast_group":
            await set_state(ADMIN_ID, None, {})
            await do_broadcast(message, "group")
            return
    if text.startswith("/"):
        return
    # Ignore bot commands; search any ordinary text in the configured group or private chat.
    await reply_search(message, text)


@dp.message(F.video | F.document | F.animation | F.audio | F.video_note | F.photo)
async def media_router(message: Message):
    if not message.from_user or not is_admin(message.from_user.id):
        return
    state, _ = await get_state(ADMIN_ID)
    if state != "ingest_upload":
        await message.answer("כדי לקלוט קבצים, פתח /admin ולחץ «התחלת קליטה».")
        return
    details = media_details(message)
    if not details:
        await message.answer("סוג הקובץ הזה אינו נתמך.")
        return
    file_id, unique_id, media_type = details
    async with pool.acquire() as con:
        count = await con.fetchval(
            "SELECT COUNT(*) FROM ingest_queue WHERE admin_id=$1 AND status='queued'", ADMIN_ID
        )
        naming = await con.fetchval(
            "SELECT COUNT(*) FROM ingest_queue WHERE admin_id=$1 AND status='naming'", ADMIN_ID
        )
        if count + naming >= MAX_BATCH:
            await message.answer("הגעת למגבלה של 50 קבצים בסבב. שלח /finish כדי להתחיל בזיהוי.")
            return
        await con.execute("""
            INSERT INTO ingest_queue(admin_id,file_id,file_unique_id,media_type,source_chat_id,source_message_id)
            VALUES($1,$2,$3,$4,$5,$6)
        """, ADMIN_ID, file_id, unique_id, media_type, message.chat.id, message.message_id)
        total = count + naming + 1
    await message.answer(f"✓ קובץ נוסף לתור. <b>{total}/{MAX_BATCH}</b>\nבסיום שלח /finish.")


@dp.callback_query(F.data.startswith("movie:"))
async def movie_callback(call: CallbackQuery):
    try:
        movie_id = int(call.data.split(":", 1)[1])
    except (ValueError, IndexError):
        await call.answer("תוצאה לא תקינה", show_alert=True); return
    async with pool.acquire() as con:
        row = await con.fetchrow("SELECT * FROM movies WHERE id=$1 AND active=TRUE", movie_id)
    if not row:
        await call.answer("הסרט כבר אינו זמין.", show_alert=True); return
    try:
        caption = f"╭━━━ ✦ MOVIE TIME ✦ ━━━╮\n🎬 <b>{esc(row['title'])}</b>\n╰━━━━━━━━━━━━━━━━━━╯" + brand_footer()
        await send_media(call.from_user.id, row["file_id"], row["media_type"], caption)
        await call.answer("הסרט נשלח לפרטי.")
    except (TelegramForbiddenError, TelegramBadRequest) as exc:
        await call.answer("צריך לפתוח את הבוט בפרטי קודם.", show_alert=True)
        try:
            await call.message.answer(
                f"╭━━━ ✦ MOVIE TIME ✦ ━━━╮\n"
                f"לא ניתן לשלוח את <b>{esc(row['title'])}</b> לפרטי.\n"
                "פתחו את הבוט בלחיצה על הכפתור ואז לחצו שוב על הסרט.\n"
                "╰━━━━━━━━━━━━━━━━━━╯" + brand_footer(),
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="↗ פתיחת Movie Time", url=f"https://t.me/{BOT_USERNAME}?start=movie_{movie_id}")]
                ])
            )
        except TelegramAPIError:
            pass
    except TelegramAPIError:
        await call.answer("השליחה נכשלה. נסו שוב מאוחר יותר.", show_alert=True)


@dp.callback_query(F.data.startswith("skip:"))
async def skip_movie(call: CallbackQuery):
    if not is_admin(call.from_user.id):
        await call.answer("אין הרשאה", show_alert=True); return
    queue_id = int(call.data.split(":")[1])
    async with pool.acquire() as con:
        await con.execute("UPDATE ingest_queue SET status='skipped' WHERE id=$1 AND status='naming'", queue_id)
    await call.answer("הקובץ דולג")
    await process_next_title(ADMIN_ID)


@dp.callback_query(F.data == "cancel_ingest")
async def cancel_ingest(call: CallbackQuery):
    if not is_admin(call.from_user.id):
        await call.answer("אין הרשאה", show_alert=True); return
    await set_state(ADMIN_ID, None, {})
    await call.answer("בוטל")
    await call.message.answer("✕ תהליך הזיהוי בוטל. הקבצים שלא נשמרו נשארו בתור.")


@dp.callback_query(F.data == "queue_status")
async def queue_status(call: CallbackQuery):
    if not is_admin(call.from_user.id):
        await call.answer("אין הרשאה", show_alert=True); return
    async with pool.acquire() as con:
        rows = await con.fetch("""
            SELECT status, COUNT(*) AS n FROM ingest_queue WHERE admin_id=$1 GROUP BY status
        """, ADMIN_ID)
    counts = {r["status"]: r["n"] for r in rows}
    await call.message.answer(
        "▤ <b>מצב תור הקליטה</b>\n\n" +
        "\n".join(f"• {esc(k)}: {v}" for k, v in counts.items()) +
        brand_footer()
    )
    await call.answer()


@dp.callback_query(F.data == "stats")
async def stats(call: CallbackQuery):
    if not is_admin(call.from_user.id):
        await call.answer("אין הרשאה", show_alert=True); return
    async with pool.acquire() as con:
        movies = await con.fetchval("SELECT COUNT(*) FROM movies WHERE active=TRUE")
        users = await con.fetchval("SELECT COUNT(*) FROM users WHERE blocked=FALSE")
        searches = await con.fetchval("SELECT COUNT(*) FROM search_logs WHERE created_at > NOW() - INTERVAL '24 hours'")
        popular = await con.fetch("""
            SELECT query, COUNT(*) n FROM search_logs
            WHERE created_at > NOW() - INTERVAL '7 days'
            GROUP BY query ORDER BY n DESC LIMIT 5
        """)
    body = "\n".join(f"• {esc(r['query'])} — {r['n']}" for r in popular) or "אין עדיין נתוני חיפוש"
    await call.message.answer(
        "╭━━━ ✦ STATISTICS ✦ ━━━╮\n\n"
        f"🎞 סרטים פעילים: <b>{movies:,}</b>\n"
        f"👥 משתמשים: <b>{users:,}</b>\n"
        f"⌕ חיפושים ב־24 שעות: <b>{searches:,}</b>\n\n"
        f"<b>חיפושים מובילים השבוע</b>\n{body}\n"
        "╰━━━━━━━━━━━━━━━━━━╯" + brand_footer()
    )
    await call.answer()


@dp.callback_query(F.data == "broadcast_start")
async def broadcast_button(call: CallbackQuery):
    if not is_admin(call.from_user.id):
        await call.answer("אין הרשאה", show_alert=True); return
    await set_state(ADMIN_ID, "broadcast_private", {})
    await call.message.answer("שלח עכשיו הודעה לשידור לכל המשתמשים שהפעילו את הבוט. /cancel לביטול.")
    await call.answer()


@dp.callback_query(F.data == "group_broadcast_start")
async def group_broadcast_button(call: CallbackQuery):
    if not is_admin(call.from_user.id):
        await call.answer("אין הרשאה", show_alert=True); return
    await set_state(ADMIN_ID, "broadcast_group", {})
    await call.message.answer("שלח עכשיו הודעה לפרסום בקבוצה המוגדרת. /cancel לביטול.")
    await call.answer()


async def do_broadcast(source: Message, target: str):
    if target == "group":
        try:
            await bot.copy_message(GROUP_ID, source.chat.id, source.message_id)
            await source.answer("✓ ההודעה נשלחה לקבוצה.")
        except TelegramAPIError as exc:
            await source.answer(f"לא ניתן לשלוח לקבוצה: {esc(str(exc)[:250])}")
        return
    async with pool.acquire() as con:
        ids = await con.fetch("SELECT user_id FROM users WHERE blocked=FALSE")
    sent = failed = 0
    status = await source.answer(f"מתחיל שידור ל־{len(ids)} משתמשים…")
    for row in ids:
        try:
            await bot.copy_message(row["user_id"], source.chat.id, source.message_id)
            sent += 1
        except TelegramForbiddenError:
            failed += 1
            async with pool.acquire() as con:
                await con.execute("UPDATE users SET blocked=TRUE WHERE user_id=$1", row["user_id"])
        except TelegramAPIError:
            failed += 1
        if (sent + failed) % 25 == 0:
            await asyncio.sleep(1.1)
    await status.edit_text(
        "╭━━━ ✦ BROADCAST REPORT ✦ ━━━╮\n\n"
        f"✓ נשלחו: <b>{sent}</b>\n"
        f"× נכשלו: <b>{failed}</b>\n"
        "╰━━━━━━━━━━━━━━━━━━╯" + brand_footer()
    )


@dp.message()
async def unknown_media_or_message(message: Message):
    # Any remaining message types are intentionally ignored to avoid noisy group replies.
    return


@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db()
    await bot.set_my_commands([
        BotCommand(command="start", description="פתיחת Movie Time"),
        BotCommand(command="admin", description="ניהול — מנהל בלבד"),
        BotCommand(command="finish", description="סיום העלאת קבצים"),
        BotCommand(command="cancel", description="ביטול פעולה"),
        BotCommand(command="broadcast", description="שידור למשתמשים — מנהל"),
        BotCommand(command="groupbroadcast", description="שידור לקבוצה — מנהל"),
    ])
    if PUBLIC_URL and WEBHOOK_SECRET:
        await bot.set_webhook(
            f"{PUBLIC_URL}/telegram/{WEBHOOK_SECRET}",
            secret_token=WEBHOOK_SECRET,
            allowed_updates=dp.resolve_used_update_types()
        )
    yield
    if pool:
        await pool.close()
    await bot.session.close()


app = FastAPI(title="Movie Time Bot", lifespan=lifespan)


@app.get("/")
async def health():
    return {"service": "Movie Time", "status": "ok"}


@app.get("/health")
async def health_check():
    return PlainTextResponse("Movie Time is alive", status_code=200)


@app.post("/telegram/{secret}")
async def telegram_webhook(secret: str, request: Request):
    if not WEBHOOK_SECRET or secret != WEBHOOK_SECRET:
        raise HTTPException(status_code=403, detail="Forbidden")
    header_secret = request.headers.get("X-Telegram-Bot-Api-Secret-Token", "")
    if header_secret != WEBHOOK_SECRET:
        raise HTTPException(status_code=403, detail="Invalid Telegram secret")
    try:
        data = await request.json()
        update = Update.model_validate(data, context={"bot": bot})
        await dp.feed_update(bot, update)
    except Exception:
        log.exception("Webhook update failed")
        raise HTTPException(status_code=500, detail="Update failed")
    return {"ok": True}
