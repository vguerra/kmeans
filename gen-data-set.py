from sklearn.datasets import make_blobs

samples = 5000
dim = 15
centers = 5

features, labels = make_blobs(n_samples=samples, centers=5, n_features=dim, random_state=0)

file_name = f"synthetic-{samples}-{dim}.data.txt"

with open(f"input/{file_name}", 'w') as data:
    for (f, l) in zip(features, labels):
        data.write(f"{','.join(['%.3f' % c for c in f])},{l}\n")
