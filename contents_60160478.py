import pandas as pd
import numpy as np

if __name__ == "__main__":
    movie=pd.read_csv('ml-latest/movies.csv')
    rating=pd.read_csv('ml-latest/ratings.csv')

movie['genres'] = movie.genres.str.split('|')
movieFeatures = movie.copy()
for index, row in movie.iterrows():
    for genre in row['genres']:
        movieFeatures.at[index, genre] = 1

movieFeatures=movieFeatures.fillna(0)
movie['year'] = movie['title'].str.extract('(\(\d\d\d\d\))',expand=False)
movie['year'] = movie['year'].str.extract('(\d\d\d\d)',expand=False)
movie['title'] = movie['title'].str.replace('(\(\d\d\d\d\))','')
movie['title'] = movie['title'].apply(lambda x:x.strip())

small_movie = movieFeatures.iloc[:5][["Adventure","Animation","Children","Comedy","Fantasy","Romance"]]

userInput = [
    {'title':'Breakfast Club, The', 'rating' : 5},
    {'title':'Toy Story', 'rating' : 3.5},
    {'title':'Jumanji', 'rating' : 2},
    {'title':'Pulp Fiction', 'rating' : 5},
    {'title':'Akira', 'rating' : 4.5}
]
inputMovies = pd.DataFrame(userInput)
inputId = movie[movie['title'].isin(inputMovies['title'].tolist())]
inputMovies = pd.merge(inputId,inputMovies)
inputMovies = inputMovies.drop('genres',1)

userMovies = movieFeatures[movieFeatures['movieId'].isin(inputMovies['movieId'].tolist())]
userMovies = userMovies.reset_index(drop=True)
userMovies = userMovies.drop('movieId',1).drop('title',1).drop('genres',1)
userProfile = userMovies.transpose().dot(inputMovies['rating'])

movieFeatures=movieFeatures.set_index(movieFeatures['movieId'])
movieFeatures=movieFeatures.drop('movieId',1).drop('title',1).drop('genres',1)

recommendationTable = ((movieFeatures*userProfile).sum(axis=1))/(userProfile.sum())
recommendationTable = recommendationTable.sort_values(ascending=False)

result = movie.loc[movie['movieId'].isin(recommendationTable.head(20).keys())]

for k,v in result.iterrows():
    print(v.title)


