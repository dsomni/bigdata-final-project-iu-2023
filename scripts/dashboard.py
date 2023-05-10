import streamlit as st
import pandas as pd
import altair as alt

st.markdown('''
<style>
section.main > div{
max-width: 1500px !important;
margin: 30px 100px;
}
</style>
''', unsafe_allow_html=True)

games = pd.read_csv("data/games.csv")
users = pd.read_csv("data/users.csv")
recommendations = pd.read_csv("data/recommendations.csv")

q1 = pd.read_csv("output/q1.csv")
q2 = pd.read_csv("output/q2.csv")
q3 = pd.read_csv("output/q3.csv")
q4 = pd.read_csv("output/q4.csv")
q5 = pd.read_csv("output/q5.csv")
q5.columns = ['published_year', 'games_number', 'reviews_number', 'avg_price_original', 'avg_price_final', 'avg_reviews_per_game']
q6 = pd.read_csv("output/q6.csv")

cv_als_config = pd.read_csv("output/cv_als_config.csv")
best_als_params = pd.read_csv("output/best_als_params.csv")
best_als_scores = pd.read_csv("output/best_als_scores.csv")

cv_rf_config = pd.read_csv("output/cv_rf_config.csv")
best_rf_params = pd.read_csv("output/best_rf_params.csv")
best_rf_scores = pd.read_csv("output/best_rf_scores.csv")
rf_features = pd.read_csv("output/rf_features.csv")

rf_recommendations = pd.read_json("output/rf_recommendations.json", lines=True)
als_recommendations = pd.read_json("output/als_recommendations.json", lines=True)

st.title("Big Data Project **2023**")
st.write('Vsevolod Klyushev (v.klyushev@innopolis.university)  \n', 
	'Dmitry Beresnev (d.beresnev@innopolis.university)')

st.markdown('---')
st.header('Descriptive Data Analysis')
st.subheader('Data Characteristics')
general_dda = pd.DataFrame(data = [
	["Games", games.shape[0]-1, games.shape[1]], 
	["Users", users.shape[0], users.shape[1]], 
	["Recommendations", recommendations.shape[0]-1, recommendations.shape[1]]
],columns = ["Table", "Instances", "Features"])
st.write(general_dda)

st.markdown('`games` table')
st.write(games.describe())

st.markdown('`users` table')
st.write(users.describe())

st.markdown('`recommendations` table')
st.write(recommendations.describe())

st.subheader('Some samples from the data')
st.markdown('`games` table')
st.write(games.head(5))

st.markdown('`users` table')
st.write(users.head(5))

st.markdown('`recommendations` table')
st.write(recommendations.head(5))

st.markdown('---')
st.header("Exploratory Data Analysis")
st.subheader('Q1')
st.markdown('How many percents of games supports each platform')
st.write(q1)
st.markdown('As we can see, all games support steam deck, so we can exclude it from further use in models. Moreover, the Windows platform seems to be the most supported.')

st.subheader('Q2')
st.markdown('The number of recommended games that do not support Windows platform')
st.write(q2)
st.markdown('As we can see, there are no recommended games that would not support the Windows platform, which means that the Windows platform support parameter is one of the key features.')

st.subheader('Q3')
st.markdown('Correlation between number of products that user have and the number of hes/her reviews')
st.write(q3)
st.markdown("As we can see, there is no correlation between number of games and reviews for users, which mean that such feature doesn't ")


st.subheader('Q4')
st.markdown('Percent of reviews with recommendation of the game')
st.write(q4)
st.markdown("That insight give us the idea that if game has review, then it's very probable, that it iis recommended.")

st.subheader('Q5')
st.markdown('Information about all games for each year of produciton')
st.write(q5)
st.markdown('As we can see, in 1998 some strange things happened: from that time one game is extremely popular.  \nMoreover, due to COVID-19 lockdown number of reviews has a peak.')

q5_chart1 = alt.Chart(q5).mark_line(point=True, strokeWidth=4)\
	.encode(x='published_year:N', y='reviews_number', tooltip=["published_year", "reviews_number"])\
	.interactive()\
	.properties(width=1200, height=500)\
	.configure_point(size=200)\
	.configure_axis(labelFontSize=20, titleFontSize=20)
st.write(q5_chart1)

q5_chart2 = alt.Chart(q5).mark_line(point=True, strokeWidth=4).encode(
    x='published_year:N', y='avg_reviews_per_game', tooltip=["published_year", "avg_reviews_per_game"]).properties(width=1200, height=500)\
	.interactive()\
        .configure_point(size=200).configure_axis(labelFontSize=20, titleFontSize=20)
st.write(q5_chart2)

st.subheader('Q6')
st.markdown("Let's check which game was released in 1988")
st.write(q6)

st.markdown('---')
st.header('Predictive Data Analytics')
st.subheader('ML Models')
st.markdown('#### 1. ALS Model')
st.markdown('- Cross validation config')
st.write(cv_als_config)
st.markdown('- Best model parameters')
st.write(best_als_params)
st.markdown('- Best model scores')
st.write(best_als_scores)
st.markdown('- Recommendations example')
st.write(als_recommendations.head(20))

st.markdown('#### 2. Random Forest Model')
st.markdown('- Columns used as features')
st.write(rf_features)
st.markdown('- Cross validation config')
st.write(cv_rf_config)
st.markdown('- Best model parameters')
st.write(best_rf_params)
st.markdown('- Best model scores')
st.write(best_rf_scores)
st.markdown('- Recommendations example')
st.write(rf_recommendations.head(20))

st.markdown('---')
st.subheader('Results')
st.markdown('We used the following metrics for estimating the performance of our models')
st.markdown('- `MAP` is a measure of how many of the recommended documents are in the set of true relevant documents, where the order of the recommendations is taken into account (i.e. penalty for highly relevant documents is higher).')
st.markdown('- `NDCG` at 5 is a measure of how many of the first 5 recommended documents are in the set of true relevant documents averaged across all users. In contrast to precision at 5, this metric takes into account the order of the recommendations (documents are assumed to be in order of decreasing relevance).')
st.markdown('As you can see, despite the ALS is the purely recommendation model, the Random Forest showed better performance on both metrics.')

st.markdown('The differense is ~6%. That is not a big surprise, since our recommendation score has only 2 values (is recommended or not). Thus, RF model with greater number of significant features shows better performance. However, ALS model uses only 3 columns of information and none of it is from **games table**.')
