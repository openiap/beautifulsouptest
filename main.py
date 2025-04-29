import os, json, logging
from openiap import Client, ClientError
import time
import asyncio
from functools import partial

import asyncio, os, openiap, traceback, zlib, json, logging
import random, time
import requests
import csv
from bs4 import BeautifulSoup

defaultwiq = ""
queue_task = None
main_loop = None  # Store the main event loop

async def keyboard_input():
    return await asyncio.get_event_loop().run_in_executor(None, input, "Enter your message: ")

def process_workitem(workitem):
    logging.info(f"Processing workitem id {workitem['id']} retry #{workitem.get('retries', 0)}")

    count = 0
    f = csv.writer(open('z-artist-names.csv', 'w'))
    f.writerow(['Name', 'Link'])
    pages = []
    for i in range(1, 5):
        url = 'https://web.archive.org/web/20121007172955/https://www.nga.gov/collection/anZ' + str(i) + '.htm'
        pages.append(url)
    for item in pages:
        count += 1
        page = requests.get(item)
        soup = BeautifulSoup(page.text, 'html.parser')

        last_links = soup.find(class_='AlphaNav')
        last_links.decompose()

        artist_name_list = soup.find(class_='BodyText')
        artist_name_list_items = artist_name_list.find_all('a')

        for artist_name in artist_name_list_items:
            names = artist_name.contents[0]
            links = 'https://web.archive.org' + artist_name.get('href')

            f.writerow([names, links])

    workitem["name"] = f"loaded {count} artist names"
    return workitem

async def process_single_workitem(client, wiq):
    """Process a single workitem when notified"""
    try:
        # Get list of files in the directory before processing
        before_files = set(os.listdir('.'))
        
        workitem = client.pop_workitem(wiq=wiq)
        if workitem is None:
            return
            
        # Process the workitem
        workitem = process_workitem(workitem)
        
        # Get list of files after processing
        after_files = set(os.listdir('.'))
        
        # Identify new files created during processing
        new_files = after_files - before_files
        new_files_list = list(new_files)
        
        # Update workitem with successful state and attach new files
        workitem["state"] = "successful"
        client.update_workitem(workitem=workitem, files=new_files_list)
        
        # Clean up by removing new files
        for file in new_files:
            try:
                os.remove(file)
                logging.info(f"Removed temporary file: {file}")
            except Exception as e:
                logging.warning(f"Failed to remove file {file}: {e}")
                
    except Exception as e:
        logging.error(f"Error processing workitem: {e}")
        if 'workitem' in locals():
            # Get list of files after error
            if 'before_files' in locals():
                after_files = set(os.listdir('.'))
                new_files = after_files - before_files
                new_files_list = list(new_files)
            else:
                new_files_list = []
                
            workitem["state"] = "retry"
            workitem["errortype"] = "application"
            workitem["errormessage"] = str(e)
            client.update_workitem(workitem=workitem, files=new_files_list)
            
            # Clean up files even in error case
            for file in new_files_list:
                try:
                    os.remove(file)
                    logging.info(f"Removed temporary file: {file}")
                except Exception as cleanup_error:
                    logging.warning(f"Failed to remove file {file}: {cleanup_error}")

def schedule_coroutine(coro):
    """Thread-safe way to schedule a coroutine on the main event loop"""
    global main_loop
    if main_loop and main_loop.is_running():
        return asyncio.run_coroutine_threadsafe(coro, main_loop)
    return None

def handle_queue(event, counter):
    """Handle queue message - only called when new workitems are available"""
    print(f"Queue event #{counter} Received")
    try:
        event_data = json.loads(event.get('data', '{}'))
        wiq = event_data.get('wiq', WIQ)
        # Process single workitem when notified
        future = schedule_coroutine(process_single_workitem(client, wiq))
        if future:
            future.add_done_callback(lambda f: f.exception() if f.exception() else None)
    except Exception as e:
        logging.error(f"Error in queue handler: {e}")

def onclientevent(result, counter):
    event = result.get("event")
    reason = result.get("reason")
    if event == "SignedIn":
        queuename = client.register_queue(queuename=WIQ, callback=handle_queue)
        print(f"Registered queue: {queuename}")
    if event == "Disconnected":
        print("Disconnected from server")
if __name__ == "__main__":
    logging.basicConfig(format="%(message)s", level=logging.INFO)
    
    WIQ = os.environ.get("wiq", defaultwiq)
    if not WIQ:
        raise ValueError("Workitem queue name (wiq) is required")

    client = Client()
    try:
        # client.enable_tracing("openiap=trace", "new")
        client.enable_tracing("openiap=info", "")
        client.connect()

        eventid = client.on_client_event(callback=onclientevent)
        print("Client event registered with id:", eventid)

        main_loop = asyncio.get_event_loop()
        try:
            main_loop.run_until_complete(keyboard_input())
        finally:
            if queue_task:
                queue_task.cancel()
            main_loop.close()
            
    except ClientError as e:
        print(f"An error occurred: {e}")
    except KeyboardInterrupt:
        print("Shutting down...")
    finally:
        if queue_task:
            queue_task.cancel()
        client.free()
