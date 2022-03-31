from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from datetime import datetime

###### maak shared variabelen aan
def getWordExcludeList(sparkContext):
    if "worldExcludeList" not in globals():
        globals()["worldExcludeList"] = sparkContext.broadcast(["Hello"])
    return globals()["worldExcludeList"]

def getDroppedWordCounters(sparkContext):
    if "droppedWordCounters" not in globals():
        globals()["droppedWordCounters"] = sparkContext.accumulator(0)
    return globals()["droppedWordCounters"]

# streaming context aanmaken (gelijkaardig aan de sparksession)
sc = SparkContext("local[2]", "network_word_count")
sc.setLogLevel("ERROR") # only show errors (anders te veel overhead bij elke iteratie warnings)
ssc = StreamingContext(sc, 5) # elke 5 seconden
ssc.checkpoint("checkpoint")

# data inlezen van poort 9999
lines = ssc.socketTextStream("localhost", 9999)
# lines gaat alle lijnen data bevatten die toekomen binnen 5 seconden intervallen
# flatmap -> alle lijnen omzetten naar 1 lijn
words = lines.flatMap(lambda line: line.split(" "))
# emit tuples (word, 1)
pairs = words.map(lambda word: (word, 1))
wordCounts = words.map(lambda word: (word, 1)).reduceByKey(lambda x, y: x+y)

def skipWords(time, rdd):
    # haal de shared variabelen op
    excludeList = getWordExcludeList(rdd.context)
    droppedWordsCounter = getDroppedWordCounters(rdd.context)
    
    def func(row):
        word = row[0]
        if word in excludeList.value:
            droppedWordsCounter.add(row[1])
            return False
        else:
            return True
    
    f = rdd.filter(func)
    
    print("# Genegeerde woorden:", droppedWordsCounter.value)
    print("Gefilterede rdd:", f.collect())
    
    pass

wordCounts.foreachRDD(skipWords)

# start applicatie
ssc.start()
ssc.awaitTermination() # zorg dat het blijft draaien
