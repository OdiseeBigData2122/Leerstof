# eerst gaan we een pydoop script maken
# een heel eenvoudige map-reduce applicaties
def mapper(_, text, writer):
    for word in text.split():
        writer.emit(word, 1)
        
def reducer(word, counts, writer):
    writer.emit(word, sum(counts))
