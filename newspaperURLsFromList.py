# %%
import pickle
import random
inf = 'periodikaausgaben_bsb_20211220.txt'
m = ['https://api.digitale-sammlungen.de/iiif/presentation/v2/' + line.rstrip('\n') + '/manifest' for line in open(inf)]
items = random.sample(m, int(len(m)/3000))
with open('newspaper_urls.pkl', 'wb') as f:
    pickle.dump(items, f)
# %%
