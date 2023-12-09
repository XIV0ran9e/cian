import os
from catboost import CatBoostRegressor
from numpy import int64
from typing import Dict
from pathlib import Path

import psycopg2
import pandas as pd
from telegram import KeyboardButton, ReplyKeyboardMarkup, Update, InlineKeyboardMarkup, InlineKeyboardButton
from telegram.ext import Application, CommandHandler, ContextTypes, MessageHandler, filters, CallbackQueryHandler

sorted_fields = ['author_type', 'city', 'deal_type', 'accommodation_type',     
                 'floor', 'floors_count', 'rooms_count', 'total_meters',
                 'district', 'underground']

class Session:
    def __init__(self, message_id) -> None:
        self.data = {
            'deal_type': 'sale',
            'accommodation_type': 'flat'
        }
        self.message_id = message_id
        self.index = 0

def load_df() -> pd.DataFrame:
    with psycopg2.connect("host='{}' port={} dbname='{}' user={} password={}".format(
        *[os.getenv(v) for v in ['BOT_PG_ADDRESS', 'BOT_PG_PORT', 'BOT_PG_DB', 'BOT_PG_USER', 'BOT_PG_PASSWORD']]
    )) as conn:
        sql = "select * from flats_clean fc where fc.date = (select max(date) from flats_clean)"
        dat = pd.read_sql_query(sql, conn)
    return dat

string_params = ['city', 'district', 'underground', 'rooms_count']
int_params = ['floor', 'floors_count', 'total_meters']
all_params = string_params + int_params

def get_categorial_values(df: pd.DataFrame) -> dict:
    keys_values = {}

    for param in string_params:
        keys_values[param] = df[param].unique()
    return keys_values

sessions: Dict[str, Session] = {}

translations = {
    'city': 'Выберите город',
    'district': 'Выберите свой район', 
    'underground': 'Выберите метро',
    'rooms_count': 'Выберите количество комнат',
    'floor': 'Введите этаж',
    'floors_count': 'Введите, сколько всего этажей',    
    'total_meters': 'Введите площадь квартиры в м^2'
}

model = CatBoostRegressor()
p = Path('/opt/files')
model.load_model(max(p.glob('model_*.json')))

df = load_df()
values = get_categorial_values(df)


async def greeting(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await upd.effective_chat.send_message(text="Здравствуйте! Это бот для оценки стоимости квартиры по её параметрам.\n"
                                    "Для начала нажмите кнопку \"Оценить мою квартиру\"",
                                    reply_markup=ReplyKeyboardMarkup([[KeyboardButton("Оценить мою квартиру")]]))

async def rate(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    chat_id = upd.effective_chat.id
    message = await upd.effective_chat.send_message(translations[string_params[0]], 
                                          reply_markup=InlineKeyboardMarkup(
                                            [
                                                [InlineKeyboardButton(text=str(v), callback_data=str(v))] for v in sorted(values[string_params[0]])
                                            ])
                                        )
    sessions[chat_id] = Session(message.message_id)

async def value_chosen(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    chat_id = upd.callback_query.message.chat_id
    session = sessions[chat_id]
    session.data[all_params[session.index]] = upd.callback_query.data
    session.index += 1
    await upd.effective_message.edit_text(translations[all_params[session.index]])
    if session.index >= len(string_params):
        pass
    else:
        await upd.effective_message.edit_reply_markup(InlineKeyboardMarkup(
                                                [
                                                    [InlineKeyboardButton(text=str(v), callback_data=str(v))] for v in sorted(values[string_params[session.index]])
                                                ]))

async def enter_value(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    chat_id = upd.effective_chat.id
    text = upd.effective_message.text
    if not isinstance(text, str) or not text.isdigit():
        await upd.effective_message.delete()
        return
    
    session = sessions[chat_id]
    session.data[all_params[session.index]] = int(text)
    session.index += 1
    await upd.effective_message.delete()
    if session.index >= len(all_params):
        await ctx.bot.edit_message_text("Данные заполнены, ожидайте", chat_id, session.message_id)
        session.data['author_type'] = 'homeowner'
        amount = int(model.predict([1] + [session.data[k] for k in sorted_fields]) / 1_000_000)
        await upd.effective_chat.send_message(f"Оценочная стоимость вашей квартиры: {amount}млн ₽")
        return
    await ctx.bot.edit_message_text(translations[all_params[session.index]], chat_id, session.message_id)

def main() -> None:
    """Run the bot."""
    application = Application.builder().token(os.getenv("BOT_TOKEN")).build()

    application.add_handler(CommandHandler("start", greeting))
    application.add_handler(MessageHandler(filters=filters.Text(["Оценить мою квартиру"]), callback=rate))
    application.add_handler(MessageHandler(filters=filters.TEXT, callback=enter_value))
    application.add_handler(CallbackQueryHandler(value_chosen))

    application.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
