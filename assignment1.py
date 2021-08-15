from mrjob.job import MRJob
from mrjob.step import MRStep

class FormatMovies (MRJob):
        def steps(self):
                return [
                        MRStep(mapper=self.mapper_get_movieratings, reducer = self.reducer_count_ratings)
                ]

        def mapper_get_movieratings(self, _, line):
                (userID, movieID, rating, timestamp)  = line.split('\t')
                yield movieID, 1
                yield rating, 2

        def reducer_count_ratings (self, key, values):
                yield key.zfill(3), str(sum(values))


if __name__ == '__main__':
        FormatMovies.run()
