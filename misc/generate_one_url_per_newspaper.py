# %%
inf = 'periodikaausgaben_bsb_20211220.txt'
m = [line.rstrip('\n') for line in open(inf)]
m
# %%
import re
d = {}
for i in m:
    id = re.sub(r'_.*', '', i)
    try:
        d[id]
    except:
        d[id] = i
    else:
        pass
# %%
l = []
for key in d:
    l.append(d[key])
urls = ['https://api.digitale-sammlungen.de/iiif/presentation/v2/' + i + '/manifest' for i in l]
import pickle
with open('newspaper_urls.pkl', 'wb') as f:
    pickle.dump(urls, f)
# %%
