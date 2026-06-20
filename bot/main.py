# region imports
import asyncio
import logging
import random
import re
from pathlib import Path
from typing import Optional, AsyncGenerator

from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict
from telethon import TelegramClient, errors, events
from telethon.custom import Message
from telethon.tl import functions, types
from telethon.tl.types import Message as TLMessage, MessageService
# endregion

# region configs
class Settings(BaseSettings):
    """Application settings from .env file"""
    
    model_config = SettingsConfigDict(
        env_file='.env',
        env_file_encoding='utf-8',
        extra='ignore',
    )
    
    SESSION_PATH: Path = Field(..., alias='SESSION_PATH')
    SESSION_NAME: SecretStr = Field(..., alias='SESSION_NAME')
    API_ID: int = Field(..., alias='API_ID')
    API_HASH: SecretStr = Field(..., alias='API_HASH')
    BOT_TOKEN: SecretStr = Field(..., alias='BOT_TOKEN')
    ADMINS_IDS: list[int] = Field(..., alias='ADMINS_IDS')
    


settings = Settings()

# Logging configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.FileHandler('app.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Telegram client connection
client: TelegramClient = TelegramClient(
    session=str(settings.SESSION_PATH / settings.SESSION_NAME.get_secret_value()),
    api_id=settings.API_ID,
    api_hash=settings.API_HASH.get_secret_value(),
)

# Task queue (max 5 concurrent tasks)
queue = asyncio.Queue(maxsize=5)
bot_info = None
# endregion

# region helpers

class TaskManagment:
    
    def __init__(self):
        self._file_path = Path('channels_info.txt')
        
    def get_task_info(self, source_channel_id: int, destination_channel_id: int) -> int:
        """
        Get the last saved offset for a task
        
        Args:
            source_channel_id: Source channel ID
            destination_channel_id: Destination channel ID
            
        Returns:
            Last message ID sent or 0
        """
        
        # Create file if it doesn't exist
        if not self._file_path.exists():
            self._file_path.touch()
            return 0
        
        try:
            with open(self._file_path, 'r', encoding='utf-8') as file:
                content = file.read()
            
            # Search pattern
            pattern = rf'{source_channel_id} to {destination_channel_id}: (\d+)'
            match = re.search(pattern, content)
            
            if match:
                last_id = int(match.group(1))
                logger.debug(f"Last ID found: {last_id}")
                return last_id
            else:
                # New task
                with open(self._file_path, 'a', encoding='utf-8') as file:
                    file.write(f'{source_channel_id} to {destination_channel_id}: 0\n')
                return 0
                
        except Exception as e:
            logger.error(f"Error reading info file: {e}")
            return 0

    def update_task_info(self, source_channel_id: int, destination_channel_id: int, last_id: int) -> bool:
        """
        Update the last sent ID for a task
        
        Args:
            source_channel_id: Source channel ID
            destination_channel_id: Destination channel ID
            last_id: Last sent message ID
            
        Returns:
            Operation success status
        """
        
        try:
            # Read current content
            if self._file_path.exists():
                with open(self._file_path, 'r', encoding='utf-8') as file:
                    lines = file.readlines()
            else:
                lines = []
            
            # Update the relevant line
            updated = False
            pattern = rf'{source_channel_id} to {destination_channel_id}: \d+'
            new_line = f'{source_channel_id} to {destination_channel_id}: {last_id}\n'
            
            for i, line in enumerate(lines):
                if re.search(pattern, line):
                    lines[i] = new_line
                    updated = True
                    break
            
            # If line not found, add it
            if not updated:
                lines.append(new_line)
            
            # Write to file
            with open(self._file_path, 'w', encoding='utf-8') as file:
                file.writelines(lines)
            
            logger.debug(f"Last ID updated: {last_id}")
            return True
            
        except Exception as e:
            logger.error(f"Error updating info file: {e}")
            return False

task_managment = TaskManagment()

async def get_posts(
    last_message_id: int,
    source_channel_id: int,
    offset_id: int = 0,
    limit: int = 100,
) -> AsyncGenerator[list, None]:
    """
    Get channel messages with pagination
    
    Args:
        source_channel_id: Source channel ID
        offset_id: Starting message ID
        limit: Messages per request
        
    Yields:
        List of messages
    """
    try:
        
        while True:
            try:
                
                messages = await client.get_messages(
                    types.PeerChannel(source_channel_id),
                    ids=[i for i in range(offset_id, offset_id + limit)]
                )
                
                # Break if no messages
                if not messages:
                    continue
                
                # Filter regular messages (not system messages)
                posts = []
                for msg in messages:
                    if isinstance(msg, TLMessage) and not isinstance(msg, MessageService):
                        posts.append(msg)
                
                # If no posts after filtering
                if not posts:
                    continue
                
                # Update offset
                offset_id = posts[-1].id
                
                logger.info(f"📥 Received {len(posts)} new messages (offset: {offset_id})")
                
                # Yield posts
                yield posts
                
                # If messages count is less than limit, we've reached the end
                if offset_id + limit > last_message_id:
                    limit = last_message_id - offset_id
                elif offset_id >= last_message_id:
                    break
                
            except errors.FloodWaitError as e:
                wait_time = e.seconds + 2
                logger.warning(f"⏳ Rate limited! Waiting {wait_time} seconds...")
                await asyncio.sleep(wait_time)
            except Exception as e:
                logger.error(f"Error fetching messages: {e}")
                break
                
    except Exception as e:
        logger.error(f"Unexpected error in get_posts: {e}")
        return


async def mirror_posts(
    source_channel_id: int,
    destination_channel_id: int,
    last_message_id: int
) -> None:
    """
    Mirror messages from one channel to another
    
    Args:
        source_channel_id: Source channel ID
        destination_channel_id: Destination channel ID
    """
    task_id = f"{source_channel_id}->{destination_channel_id}"
    logger.info(f"🔄 Starting task {task_id}")
    
    try:
        # Get last sent message ID
        start_offset = task_managment.get_task_info(source_channel_id, destination_channel_id)
        logger.info(f"📌 Starting from ID: {start_offset}")
        
        total_sent = 0
        
        # Fetch and send messages
        async for posts in get_posts(
            source_channel_id=source_channel_id,
            offset_id=start_offset,
            limit=100,
            last_message_id=last_message_id
        ):
            for msg in posts:
                try:
                    # Send message to destination channel
                    await client.forward_messages(
                        from_peer=types.PeerChannel(source_channel_id),
                        entity=types.PeerChannel(destination_channel_id),
                        messages=msg.id,
                        drop_author=True,
                    )
                    
                    total_sent += 1
                    
                    # Update last sent message ID
                    task_managment.update_task_info(source_channel_id, destination_channel_id, msg.id)
                    
                    # Random delay between 0.5 and 1.5 seconds
                    delay = round(0.5 + random.random(), 2)
                    await asyncio.sleep(delay)
                    
                    # Log every 10 messages
                    if total_sent % 10 == 0:
                        logger.info(f"📤 Sent {total_sent} messages in task {task_id}")
                    
                except errors.FloodWaitError as e:
                    wait_time = e.seconds + 2
                    logger.warning(f"⏳ Rate limited while sending! Waiting {wait_time} seconds...")
                    await asyncio.sleep(wait_time)
                except Exception as e:
                    logger.error(f"Error sending message {msg.id}: {e}")
                    continue
        
        logger.info(f"✅ Task {task_id} completed! Total: {total_sent} messages")
        
    except Exception as e:
        logger.error(f"❌ Error in task {task_id}: {e}")
# endregion

# region handlers
@client.on(events.NewMessage(
    incoming=True,
    outgoing=False,
    pattern=r'^/mirror -100\d+$',
    func=lambda e: e.is_channel,
))
async def channel_mirror_handler(event: Message) -> None:
    """
    Handler for starting mirror task
    """
    try:
        
        # Check admin
        async for admin in client.iter_participants(event.chat, filter=types.ChannelParticipantsAdmins, search=event.message.post_author):
            if admin.id in settings.ADMINS_IDS:
                break
        else:
            return
        
        # Extract IDs
        parts = event.raw_text.split()
        if len(parts) < 2:
            await logger.warning("❌ Invalid format. Use: /mirror -100123456789")
            return
        
        destination_channel_id = int(parts[1])
        source_channel_id = event.chat_id
        
        logger.info(f"📋 New task: {source_channel_id} -> {destination_channel_id} by {event.sender_id}")
        
        # Check if bot is admin in destination channel
        try:
            bot = await client.get_me()
            participant = await client(functions.channels.GetParticipantRequest(
                channel=types.PeerChannel(destination_channel_id),
                participant=bot
            ))
            
            if not isinstance(participant.participant, (types.ChannelParticipantAdmin, types.ChannelParticipantCreator)):
                logger.error(f"❌ Bot is not admin in destination channel {destination_channel_id}!")
                return
                
        except errors.UserNotParticipantError:
            logger.error(f"❌ Bot is not a member of destination channel {destination_channel_id}!")
            return
        except Exception as e:
            logger.error(f"❌ Error checking admin status: {e}")
            return
        
        # Check queue capacity
        if queue.full():
            logger.warning(f"⚠️ Queue is full! Task {source_channel_id}->{destination_channel_id} rejected")
            return
        
        # Add task to queue
        task = asyncio.create_task(
            mirror_posts(source_channel_id, destination_channel_id)
        )
        await queue.put(task)
        
        logger.info(f"✅ Task {source_channel_id}->{destination_channel_id} added to queue, 📍 Status: In queue ({queue.qsize()}/{queue.maxsize})")
        
        # Send reply and delete after 3 seconds
        reply_msg = await event.reply(
            f"✅ Mirror task started!\n"
            f"📤 From: {source_channel_id}\n"
            f"📥 To: {destination_channel_id}\n"
            f"📍 Status: In queue ({queue.qsize()}/{queue.maxsize})"
        )
        await asyncio.sleep(1)
        await reply_msg.delete()
        
    except ValueError as e:
        logger.error(f"❌ Invalid ID format: {e}")
    except Exception as e:
        logger.error(f"❌ Handler error: {e}")
    finally:
        raise events.StopPropagation


@client.on(events.NewMessage(
    incoming=True,
    outgoing=False,
    from_users=settings.ADMINS_IDS,
    pattern=r'^/status$',
    func=lambda e: e.is_private,
))
async def status_handler(event: Message) -> None:
    """Display bot status"""
    status_text = (
        f"📊 **Bot Status**\n"
        f"🔹 Tasks in queue: {queue.qsize()}/{queue.maxsize}\n"
        f"🔹 Status: {'🟢 Online' if client.is_connected() else '🔴 Offline'}\n"
        f"🔹 Bot: {bot_info.first_name if bot_info else 'Unknown'}"
    )
    await event.reply(status_text)


@client.on(events.NewMessage(
    incoming=True,
    outgoing=False,
    from_users=settings.ADMINS_IDS,
    pattern=r'^/stop$',
    func=lambda e: e.is_private,
))
async def stop_handler(event: Message) -> None:
    """Stop the bot"""
    logger.info("🛑 Received stop command")
    await event.reply("🛑 Stopping bot...")
    await client.disconnect()
# endregion

# region run
async def main() -> None:
    """Main function"""
    global bot_info
    
    try:
        # Start the bot
        await client.start(bot_token=settings.BOT_TOKEN.get_secret_value())
        bot_info = await client.get_me()
        
        logger.info("=" * 50)
        logger.info(f"✅ Bot is online!")
        logger.info(f"🤖 Name: {bot_info.first_name}")
        logger.info(f"🆔 ID: {bot_info.id}")
        logger.info(f"👥 Admins: {settings.ADMINS_IDS}")
        logger.info("=" * 50)
        
        # Queue processor
        async def queue_processor():
            """Process tasks in queue"""
            while True:
                try:
                    task = await queue.get()
                    await task
                    queue.task_done()
                except asyncio.CancelledError:
                    break
                except Exception as e:
                    logger.error(f"❌ Error processing task: {e}")
                    queue.task_done()
        
        # Start queue processor in background
        processor_task = asyncio.create_task(queue_processor())
        
        # Wait for bot to disconnect
        await client.run_until_disconnected()
        
        # Cleanup
        processor_task.cancel()
        await client.disconnect()
        
    except Exception as e:
        logger.critical(f"❌ Critical error in main: {e}")
        raise


if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("👋 Bot stopped with Ctrl+C")
    except Exception as e:
        logger.critical(f"❌ Execution error: {e}")
# endregion
