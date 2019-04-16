
import mrs
import string

class InvertedIndex(mrs.MapReduce):

    def map(self, line_num, line_text):
        
        for word in line_text.split():
            word = word.strip(string.punctuation).lower()
            if word:
                yield (word, line_num)

    def reduce(self, word, counts):
	
        yield list(counts)
	

if __name__ == '__main__':
    mrs.main(InvertedIndex)

