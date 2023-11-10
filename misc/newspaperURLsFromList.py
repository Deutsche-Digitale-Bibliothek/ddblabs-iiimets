# %%
import pickle

inf = "periodikaausgaben_bsb_20211220.txt"
m = [
    "https://api.digitale-sammlungen.de/iiif/presentation/v2/"
    + line.rstrip("\n")
    + "/manifest"
    for line in open(inf)
]

with open("periodikaausgaben_bsb_20211220.txt.pkl", "wb") as f:
    pickle.dump(m, f)
# %%
