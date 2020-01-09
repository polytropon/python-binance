import telegram
import os
token = os.getenv("TELEGRAM_TOKEN")
token = os.environ["TELEGRAM_BINANCE_TOKEN"]
assert ":" in token
bot = telegram.Bot(token=token)

## Solution:
##https://github.com/python-telegram-bot/python-telegram-bot/issues/538
def sendTelegram(message):
    print("sending telegram message: " + message)
    bot.send_message(chat_id=-1001348217980, text=message)
 
##    for x in bot.get_updates():
##        print("x is "  + str(x))
##        bot.send_message(chat_id=x.message.chat_id, text=message)
##        print("sent: " + str(message))

if __name__ == '__main__':
    sendTelegram("Test")
