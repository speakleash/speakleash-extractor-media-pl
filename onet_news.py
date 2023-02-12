
from multiprocessing import set_start_method

from multiprocessing.pool import Pool
import requests
import os
from lm_dataformat import Archive
import shutil
import spacy
import json
import glob
import urllib.robotparser
import time
from datetime import datetime
import justext
from bs4 import BeautifulSoup
import datetime

    


def query_onet_news():

          
    date = datetime.date(2000,1,1)
    url = 'https://wiadomosci.onet.pl/archiwum/'


    while date <= datetime.date.today():
        response = requests.get(url+date.strftime("%Y-%m-%d"))
        
        if not response.ok:
            print("Archive response error...retrying")
            time.sleep(10)
            response = requests.get(url+date.strftime("%Y-%m-%d"))

        if response.ok:
            soup = BeautifulSoup(response.content, "html.parser")
            results = soup.find_all("ul", class_="dayInArchive")
            for res in results:
                for li in res.find_all("li"):
                
                    for a in li.find_all("a", class_="itemTitle"):                
                        title = a.text
                        item_url = a["href"]
                        yield title, item_url
        date = date + datetime.timedelta(days=1)
    return


def get_item_text(url:str):

  response = requests.get(url)
  text = ''

  if not response.ok:
    print("Item response error...retrying")
    time.sleep(10)
    response = requests.get(url)

  if response.ok:
    paragraphs = justext.justext(response.content, justext.get_stoplist("Polish"), max_heading_distance=150, length_low=70, length_high=140, stopwords_low=0.2, stopwords_high=0.3, max_link_density=0.4)
   
    for paragraph in paragraphs:
      if not paragraph.is_boilerplate:
        text += (paragraph.text+"\n")        
   
  else:
    print("Error -> "+url)
   
  return text



def get_word_stats(txt):
    if not txt:
        return 0, 0, 0, 0, 0, 0, 0

    sentences = 0
    words = 0
    verbs = 0
    nouns = 0
    punctuations = 0
    symbols = 0
    stopwords = 0

    doc = nlp(txt)

    sentences = len(list(doc.sents))
    words = len([token.text for token in doc if not token.is_punct])
    nouns = len([token.text for token in doc if (not token.is_stop and not token.is_punct and token.pos_ == "NOUN")])
    verbs = len([token.text for token in doc if (not token.is_stop and not token.is_punct and token.pos_ == "VERB")])
    punctuations = len([token.text for token in doc if (token.is_punct or token.pos_ == "PUNCT")])
    symbols = len([token.text for token in doc if (token.pos_ == "SYM")])
    stopwords = len([token.text for token in doc if token.is_stop])

    return sentences, words, verbs, nouns, punctuations, symbols, stopwords

def process_item(book_info):

    title = book_info[0]
    item_url = book_info[1]
    meta = {}
    txt = ''
    ok = False
    print(title)
    try:
        txt = get_item_text(item_url)
        
        if txt:
            ok = True
            l = len(txt.strip())
            if l > 100000:
                nlp.max_length = len(txt) + 100
            sentences, words, verbs, nouns, punctuations, symbols, stopwords = get_word_stats(txt.strip())
            meta = {'url' : item_url, 'title': title, 'length': l, 'sentences': sentences, 'words': words, 'verbs': verbs, 'nouns': nouns, 'punctuations': punctuations, 'symbols': symbols, 'stopwords': stopwords}
    except:
        ok = False
        print("Error processing item -> "+item_url)
    finally:
        return ok, txt.strip(), meta

    

def initialize_worker():

    print('Initializing worker...')   

    #Each worker node needs to have its own resources.
    global nlp
    global rp
   
    rp = urllib.robotparser.RobotFileParser()
    rp.set_url('https://www.komputerswiat.pl/robots.txt')
    rp.read()


    #Disabling some unused model features speeds things up to 20%
    nlp = spacy.load("pl_core_news_md", disable=('ner','lemmatizer','textcat','entity_linker'))
      


if __name__ == '__main__':

    set_start_method("spawn")
    
    
    start_time = time.time()


    ar = Archive('./data')
    
    file_name_zst = './onet_pl_news_corpus.jsonl.zst'
    file_name_manifest = './onet_pl_news_corpus.manifest'

    total_len = 0
    total_docs = 0
    total_sentences = 0
    total_words = 0
    total_verbs = 0
    total_nouns = 0
    total_punctuations = 0
    total_symbols = 0
    total_stopwords = 0

    total = 0
    added = 0


    # create and configure the process pool. All available resources are used by default
    


    with Pool(initializer=initialize_worker, processes=os.cpu_count(), maxtasksperchild=1000) as pool:
        # issue tasks to the process pool
        for ok, txt, meta in pool.imap(func=process_item, iterable=query_onet_news(), chunksize=os.cpu_count()):
            total += 1
            if ok:
                total_words += meta['words']
                total_verbs += meta['verbs']
                total_nouns += meta['nouns']
                total_len += meta['length']
                total_docs += 1
                total_sentences += meta['sentences']
                total_punctuations += meta['punctuations']
                total_symbols += meta['symbols']
                total_stopwords += meta['stopwords']
                ar.add_data(txt, meta = meta)
                added += 1
                print("Added "+str(total)+"/"+str(added)+" " + meta.get('url'))
            else:
                print("Skipping "+str(total)+"/"+str(added))


        # Close the process pool
        pool.close()
        # Wait for all tasks to complete
        pool.join()
    ar.commit()


    data_files= glob.glob('./data/*')
    file_size = 0

    #This solves an issue where data_files remains locked after ar commiting, causing error on cleanup
    ar = None

    for f in data_files:
        if f.endswith('.zst'):
            shutil.copy(f, os.path.join(file_name_zst))
            file_size = os.path.getsize(file_name_zst)

        os.remove(f)

    manifest = {"project" : "SpeakLeash", "name": "onet_pl_news_corpus", "description": "Collection of news from onet.pl corpus", "license": "(c) www.onet.pl", "Category": "Internet", "language": "pl", "file_size" : file_size, "sources": [{"name": "onet_pl_news_corpus", "url": "https://wwww.onet.pl", "license": "(c) www.onet.pl"}], "stats": {"documents": total_docs, "sentences": total_sentences, "words" : total_words, "nouns" : total_nouns, "verbs" : total_verbs, "characters": total_len, "punctuations" : total_punctuations, "symbols" : total_symbols, "stopwords": total_stopwords}}
    json_manifest = json.dumps(manifest, indent = 4) 

    with open(file_name_manifest, 'w') as mf:
        mf.write(json_manifest)

    print(time.time()-start_time)