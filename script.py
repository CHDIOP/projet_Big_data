#########  BIG DATA #########

# Cheikh Diop


# import package
from pyspark import SparkContext

# session sparkContext
sc = SparkContext(master="local",appName="Spark Demo")
# importation du fichier sample.txt
file = sc.textFile("sample.txt")

words = file.flatMap(lambda l : l.split(" "))
Counts= words.countByValue()

# affichage des mots
for word, count in Counts.items():
    print("{} :{}".format(word,count))



