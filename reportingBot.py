import os
import discord
from dotenv import load_dotenv
import stockDataCollector

load_dotenv()
TOKEN = os.getenv("BOT_TOKEN")
GUILD = os.getenv("DISCORD_GUILD")

client = discord.Client(intents=discord.Intents.all())
toolkit = {"hist": stockDataCollector.getStockHistory}


@client.event
async def on_ready():
    print("Bot connected")


@client.event
async def on_message(message):
    if message.author == client.user:
        return

    m = message.content
    print(m)
    print
    if m.startswith("c^ "):
        m = m[3:]
        print(m)
        r = toolkit[m]("TSLA", "1d", "1h")
        await message.channel.send(r.head())


client.run(TOKEN)
