### Sentiment Analysis using Python and Connect to Oracle DB

![alt text](images/schema_model.jpg "Logo Title Text 1")

I created this program when I have a POC about social media analytics. The client wants to analytic the streaming data from Kafka for each social media sources, likes Twitter, Instagram, Facebook, Gplus, and News. They want the data that has been analyzed to be stored in the Oracle database. So, i use the cx_Oracle which is a Python extension module that enables us to access the Oracle Database.

Keterangan :
fb_to_db.py : Codingan untuk menganalisis sentiment dan entity dari fb, kemudian langsung ditembakkan ke database Oracle
twitter_to_db.py : Codingan untuk menganalisis sentiment dan entity dari twitter, kemudian langsung ditembakkan ke database Oracle
gplus_to_db.py : Codingan untuk menganalisis sentiment dan entity dari gplus, kemudian langsung ditembakkan ke database Oracle
instagram_to_db.py : Codingan untuk menganalisis sentiment dan entity dari instagram, kemudian langsung ditembakkan ke database Oracle
news_to_db.py : Codingan untuk menganalisis sentiment dan entity dari news, kemudian langsung ditembakkan ke database Oracle

DataPreprocessing : berisi codingan & library utk mengolah data teks sebelum diproses ke sentiment maupun entity
dictionary.pickle : berisi kode setiap angka yang di-mapping untuk menemukan model Word2Vector nya
final_embeddings.py : berisi model word2vector
Location.txt : berisi lokasi daerah-daerah di Indonesia beserta dengan longitude dan lattitude-nya
models : model hasil pembelajaran LSTM-nya
