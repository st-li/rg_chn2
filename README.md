# This is for gether Chinese guys' information. In this version, I made the following change:

* Make the start_urls list form a csv file
~~~
start_urls = pd.read_csv('/data/pure_chn_link.csv', header=None).ix[:, 0].tolist()
~~~
* Join with mongoDB to find the urls which are already crawled and delete them from the start_urls list