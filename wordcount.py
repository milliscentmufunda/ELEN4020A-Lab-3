
import mrs
import string



class wordcount(mrs.MapReduce):
    """Count the number of occurrences of each word in a set of documents.

    Word Count is the classic "hello world" MapReduce program. This is a
    working example and is provided to demonstrate a simple Mrs program. It is
    further explained in the tutorials provided in the docs directory.
    """
    
    def map(self, line_num, line_text):
        
        for word in line_text.split():
            word = word.strip(string.punctuation).lower()
            if word:
                    yield (word, 1)

       

    def reduce(self, word, counts):
            yield sum(counts)


if __name__ == '__main__':
    mrs.main(wordcount)

# vim: et sw=4 sts=4
