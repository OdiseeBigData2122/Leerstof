from mrjob.job import MRJob

# klasse die overerft van MRJob
class MRWordCount(MRJob):
    # functie voor de map-fase in mapreduce
    # _ wil zeggen dat dit argument niet gebruikt wordt/niet belangrijk is
    def mapper(self, _, line):
        for word in line.split():
            # hier moeten we yield gebruiken
            # yield is een beetje als een return, maar functie blijft runnen
            # alle return waarden worden dan in een lijst gestoken en gereturned
            yield (word, 1)
    
    # functie voor de refuce-fase
    def reducer(self, word, counts):
        yield (word, sum(counts))
        
## zeggen dat deze file de main file is
if __name__ == "__main__":
    MRWordCount.run()
