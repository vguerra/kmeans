from sklearn.cluster import KMeans
# from sklearn import datasets
import numpy as np

data = np.genfromtxt('input/iris.data.txt', delimiter=',')
data = data[:,:-1]

kmeans = KMeans(n_clusters=3, init='random').fit(data)
print(kmeans.inertia_)
print(kmeans.cluster_centers_)