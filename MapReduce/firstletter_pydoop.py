import pydoop.mapreduce.api as api
import pydoop.mapreduce.pipes as pipes
from itertools import tee

class Mapper(api.Mapper):
    
    def __init__(self, context):
        #constructor
        #de super voert de constructor uit van de bovenliggende klasse (api.Mapper)
        super(Mapper, self).__init__(context)
        context.set_status("initializing mapper")
        self.input_words = context.get_counter("WORDCOUNT", "INPUT_WORDS")
    
    # key-values zitten in de context
    def map(self, context):
        # context.value is de lijn die we lezen
        for w in context.value.split():
            print(w)
            if len(w)>0 and w[0].isalpha():
                context.emit(w[0].lower(), 1)
                context.increment_counter(self.input_words, 1)
            # er kunnen meerdere zaken ge-emit worden
            # besteed wel aandacht aan de gekozen key
            # zodat er geen conflict is met de andere keys
            # bijvoorbeeld bij first letter -> kies geen letter
            # in het wordcount example -> kies geen woord maar gebruik twee woorden met een spatie
            context.emit("lengte woord", len(w))
    
class Reducer(api.Reducer):
    
    def __init__(self, context):
        #constructor
        #de super voert de constructor uit van de bovenliggende klasse (api.Mapper)
        super(Reducer, self).__init__(context)
        context.set_status("initializing reducer")
        self.unique_letters = context.get_counter("WORDCOUNT", "DIFF_FIRST_LETTERS")
        
    # key-values zitten in de context    
    def reduce(self, context):
        if context.key == "lengte woord":
            it0, it1, it2 = tee(context.values, 3)
            som = 0
            aantal = 0
            for val in it0:
                aantal += 1
                som += val
                
            print("gemiddelde", som/aantal)
            context.emit("gemiddelde", som/aantal)
            
            # dit gaat niet zomaar werken
            # context.values is een iterator en geen lijst
            # iterator kan 1 keer over een lijst/dictionary gaan lopen/itereren
            # om dit op te lossen moeten we de iterator gaan dupliceren / kopieen van maken
            #aantal = len(context.values)
            #som = sum(context.values)
            aantal = len(list(it1))
            som = sum(it2)
            print("mean", som/aantal)
            context.emit("mean", som/aantal)
        else:
            context.emit(context.key, sum(context.values))
            context.increment_counter(self.unique_letters, 1)
        
FACTORY = pipes.Factory(Mapper, reducer_class=Reducer)

def main():
    pipes.run_task(FACTORY)

## zeggen dat deze file de main file is
if __name__ == "__main__":
    main()
