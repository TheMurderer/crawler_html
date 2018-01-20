import _thread, queue, time, requests
import pymongo
from bs4 import BeautifulSoup

class Crawler(object):
    """ Example class Crawler. Author: Raul Salazar """


    def __init__(self):
        self.links = queue.Queue(100)
        self.links.put("https://es.wikipedia.org/wiki/Wikipedia:Portada")
        self.counter = 0 # Counter for debug purpose
        self.nameFile="result_crawler.txt"
        self.writeQueue = queue.Queue()
        self.__mongoStorage = False
        if self.__mongoStorage:
            client = pymongo.MongoClient('localhost', 27017)
            self.collectionMongo = client.crawler.linksHTML
            self.collectionMongo.create_index([('url', pymongo.TEXT )])


    def open(self, current_page):
        """Method who proccess a individual current_page and save links in queue"""
        print (self.counter, ":", current_page)
        res = requests.get(current_page)
        bsObj = BeautifulSoup(res.text, "html.parser")

        if self.__mongoStorage:
            try:

                if self.collectionMongo.find({'url':current_page}).count() == 0:
                    self.collectionMongo.insert({'url':current_page,"contain_html":res.text})

            except Exception:
                print ("Error saving in Mongo" + current_page)
                pass
        else:
            self.writeQueue.put(current_page+" :: "+res.text+"\n")

        try:
            for a in bsObj.findAll('a'):
                gRef = a.get('href')

                if gRef is not None:
                    if "http" not in gRef:
                        self.links.put("https://es.wikipedia.org" + gRef, 1000)
                    else:
                        self.links.put(gRef)


        except Exception:  # Magnificent exception handling
            print ( 'Error add link to queue')
            pass

    def __writeFile(self):
        """ Method to write results in file """
        outputfile = open(self.nameFile, 'a')
        while (self.writeQueue.qsize()):
            outputfile.write(self.writeQueue.get())
        outputfile.flush()
        outputfile.close()


    def run(self):
        """Method who generate thread"""

        while not self.links.empty():
            self.counter += 1
            _thread.start_new_thread(self.open,(self.links.get(True,1000),))
            time.sleep(0.5)
            if not self.__mongoStorage:
                self.__writeFile()



if __name__ == '__main__' :

    C = Crawler()
    C.run()







