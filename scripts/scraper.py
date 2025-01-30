import os
from telethon import TelegramClient, events
from dotenv import load_dotenv
import logging
import psycopg2

load_dotenv()

# Configure logging
logging.basicConfig(
    filename='scraper.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# PostgreSQL connection
conn = psycopg2.connect(
    dbname="telegram_scrapped",
    user="postgres",
    password="root",
    host="localhost"
)
cursor = conn.cursor()

# Schema migration
try:
    cursor.execute('''
        ALTER TABLE messages 
        ADD COLUMN IF NOT EXISTS message_id BIGINT
    ''')
    cursor.execute('''
        ALTER TABLE messages 
        ADD CONSTRAINT IF NOT EXISTS channel_message_unique 
        UNIQUE (channel, message_id)
    ''')
    conn.commit()
except Exception as e:
    logging.error(f"Schema migration error: {e}")
    conn.rollback()

# Create tables if not exists
cursor.execute('''
    CREATE TABLE IF NOT EXISTS messages (
        id SERIAL PRIMARY KEY,
        message_id BIGINT NOT NULL,
        channel VARCHAR(255) NOT NULL,
        text TEXT,
        date TIMESTAMP,
        image_path VARCHAR(255),
        UNIQUE (channel, message_id)
    )
''')
conn.commit()

# Telegram client setup
client = TelegramClient('session_name', os.getenv('API_ID'), os.getenv('API_HASH'))

async def scrape_old_messages():
    """Scrape historical messages from specified channels."""
    channels = ['DoctorsET', 'Chemed', 'lobelia4cosmetics', 'yetenaweg', 'EAHCI']
    
    for channel_username in channels:
        try:
            channel = await client.get_entity(channel_username)
            actual_username = channel.username
            image_dir = os.path.join("images", actual_username)
            os.makedirs(image_dir, exist_ok=True)

            async for message in client.iter_messages(channel):
                try:
                    image_path = None
                    if message.media:
                        image_filename = f"{message.id}.jpg"
                        image_path = os.path.join(image_dir, image_filename)
                        await message.download_media(image_path)

                    cursor.execute('''
                        INSERT INTO messages (message_id, channel, text, date, image_path)
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (channel, message_id) DO NOTHING
                    ''', (message.id, actual_username, message.text, message.date, image_path))
                    conn.commit()
                    
                    logging.info(f"Old message {message.id} from {actual_username} saved.")
                except Exception as e:
                    logging.error(f"Error processing message {message.id} from {actual_username}: {e}")
                    conn.rollback()

        except Exception as e:
            logging.error(f"Error scraping old messages from {channel_username}: {e}")
            conn.rollback()

@client.on(events.NewMessage(chats=['DoctorsET', 'Chemed', 'lobelia4cosmetics', 'yetenaweg', 'EAHCI']))
async def handler(event):
    try:
        message = event.message
        channel_username = event.chat.username
        image_path = None

        if message.media:
            image_dir = os.path.join("images", channel_username)
            os.makedirs(image_dir, exist_ok=True)
            image_filename = f"{message.id}.jpg"
            image_path = os.path.join(image_dir, image_filename)
            await message.download_media(image_path)
        
        cursor.execute('''
            INSERT INTO messages (message_id, channel, text, date, image_path)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (channel, message_id) DO NOTHING
        ''', (message.id, channel_username, message.text, message.date, image_path))
        conn.commit()
        logging.info(f"New message {message.id} from {channel_username} saved.")
    except Exception as e:
        logging.error(f"Error processing new message {message.id}: {e}")
        conn.rollback()

async def main():
    await client.start()
    await scrape_old_messages()
    await client.run_until_disconnected()

if __name__ == "__main__":
    try:
        with client:
            client.loop.run_until_complete(main())
    except KeyboardInterrupt:
        logging.info("Script stopped by user")
    finally:
        cursor.close()
        conn.close()
        logging.info("Database connection closed")