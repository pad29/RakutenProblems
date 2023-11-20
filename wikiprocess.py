# This parsing code is adopted from https://dennisforbes.ca/articles/processing_wikipedia_dumps_with_python.html

import bz2
import queue
import wikitextparser
import multiprocessing
import wikiindex
import xml.sax
import io
import sqlite3

#articles_source = "wikipedia_dump/fatwiki-20231101-pages-articles-multistream.xml.bz2"
#index_source = "wikipedia_dump/fatwiki-20231101-pages-articles-multistream-index.txt.bz2"
#database_name = "wikipedia_database_fat.db"
articles_source = "wikipedia_dump/jawiki-20231101-pages-articles-multistream.xml.bz2"
index_source = "wikipedia_dump/jawiki-20231101-pages-articles-multistream-index.txt.bz2"
database_name = "wikipedia_database_jp.db"

def process_worker(work_queue, work_queue_lock):
    offsets_processed = 0
    stream_filehandle = open(articles_source, "rb")
    try:

        while True:
            try:
                work_queue_lock.acquire()
                stream_offset = work_queue.get(block=False)
            finally:
                work_queue_lock.release()

            stream_filehandle.seek(stream_offset)
            decompressor = bz2.BZ2Decompressor()

            output = [b'<pages>']
            while not decompressor.eof:
                output.append(decompressor.decompress(stream_filehandle.read(65536)))
            output.append(b'</pages>')

            contents = b''.join(output)
            process_stream_contents(contents)
            offsets_processed += 1
    except queue.Empty:
        return
    finally:
        print("Worker process shutting down after processing {} offsets".format(offsets_processed))
        stream_filehandle.close()


def process_page(page_id, page_ns, page_title, page_redirect, page_content, cursor):
    
    if page_redirect is None:
        #page_parsed = wikitextparser.parse(page_content)
        cursor.execute('INSERT INTO WikipediaTitles VALUES (?, ?)', (page_id, page_title))



class XMLSAXParser(xml.sax.ContentHandler):
    def __init__(self, cursor):
        super().__init__()

        self.read_stack = []
        self.page_id = None
        self.page_title = None
        self.page_redirect = None
        self.page_ns = None
        self.page_content = None

        self.page_count = 0
        self.in_page = False

        self.cursor = cursor

    def startElement(self, tag_name, attributes):

        self.text_aggregate = []

        if tag_name == "page":
            self.page_redirect = None
            self.page_title = None
            self.page_id = None
            self.page_ns = None
            self.page_content = None
            self.in_page = True
        else:
            if (tag_name == "redirect") and (self.read_stack[-1] == "page"):
                self.page_redirect = attributes["title"]

        self.read_stack.append(tag_name)

    def endElement(self, tag_name):
        if (len(self.read_stack) > 0) and (tag_name == self.read_stack[-1]):
            del self.read_stack[-1]
        else:
            raise Exception("Tag ({}) does not match open tag ({}).".format(tag_name, self.read_stack[-1]))

        element_string = ''.join(self.text_aggregate)

        if tag_name == "page":
            self.in_page = False
            process_page(self.page_id, self.page_ns, self.page_title, self.page_redirect, self.page_content, self.cursor)
        else:
            if self.in_page:
                if self.read_stack[-1] == "page":
                    if tag_name == "title":
                        self.page_title = element_string
                    elif (tag_name == "id") and self.read_stack[-1]:
                        self.page_id = int(element_string)
                    elif tag_name == "ns":
                        self.page_ns = int(element_string)
                elif self.read_stack[-1] == "revision":
                    # the actual page contents exist as a revision
                    if tag_name == "text":
                        self.page_content = element_string

    text_aggregate = []

    def characters(self, content):
        if self.in_page:
            self.text_aggregate.append(content)


def process_stream_contents(manyPages):
    connection = sqlite3.connect(database_name)
    cursor = connection.cursor()

    # Create table if it doesn't exist
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS WikipediaTitles (
            article_id INTEGER PRIMARY KEY,
            title TEXT
        )
    ''')

    reader = XMLSAXParser(cursor)
    try:
        byte_stream = io.BytesIO(manyPages)
        xml.sax.parse(byte_stream, reader)
    finally:
        byte_stream.close()
        connection.commit()
        connection.close()


def main():
    try:

        sorted_stream_offsets = wikiindex.process_index_file(index_source)
        if (sorted_stream_offsets is None) or (len(sorted_stream_offsets) < 1):
            raise Exception("Index file unsuccessful")

        process_count = multiprocessing.cpu_count()

        work_queue = multiprocessing.Queue()
        work_queue_lock = multiprocessing.Lock()

        [work_queue.put(x) for x in sorted_stream_offsets]

        jobs = []

        for i in range(process_count):
            p = multiprocessing.Process(target=process_worker, args=(work_queue,work_queue_lock))
            p.start()
            jobs.append(p)

        for j in jobs:
            j.join()

    except Exception as e:
        print(e)


if __name__ == "__main__":
    multiprocessing.freeze_support()
    main()