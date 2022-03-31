from pyspark import SparkContext
from pyspark.streaming import StreamingContext

# streaming context aanmaken (gelijkaardig aan de sparksession)
sc = SparkContext("local[2]", "network_word_count")
sc.setLogLevel("ERROR") # only show errors (anders te veel overhead bij elke iteratie warnings)
ssc = StreamingContext(sc, 1) # elke 1 second (aangezien het om de twee seconden moet opschijven en je window 5 seconden is)
# als het meer dan 1 second is moet je een batch in twee knippen
ssc.checkpoint("temp_state")

# data inlezen van poort 9999
lines = ssc.socketTextStream("localhost", 9999)
windowLines = lines.window(5, 2)
# lines gaat alle lijnen data bevatten die toekomen binnen 5 seconden intervallen
# flatmap -> alle lijnen omzetten naar 1 lijn
words = windowLines.flatMap(lambda line: line.split(" "))
# emit tuples (word, 1)
pairs = words.map(lambda word: (len(word), 1))
# som alle 1-tjes van elk woord/key
wordCount = pairs.reduceByKey(lambda x, y: x + y)
# genereer output
wordCount.pprint()

# totaal aantal woorden
def updateFunctie(new_values, last_sum):
    return sum(new_values) + (last_sum or 0)

total_WC = pairs.updateStateByKey(updateFunctie)
# total_WC -> (word, total_count)
# total_counts optellen
# eerst eruit halen met de map
# reduce om ze op te tellen
total_WC.map(lambda kv1: kv1[1]).reduce(lambda x,y: x+y).pprint()

# start applicatie
ssc.start()
ssc.awaitTermination() # zorg dat het blijft draaien
