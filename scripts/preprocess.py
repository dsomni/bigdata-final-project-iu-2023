import pandas as pd

recommendations_df = pd.read_csv("./data/recommendations.csv")
users_df = pd.read_csv("./data/users.csv")

users_reduced_df = users_df[::5]

users_reduced_df.to_csv("./data/users.csv", index=False)

available_users = users_reduced_df["user_id"].values.tolist()

recommendations_reduced_df = recommendations_df[
    recommendations_df["user_id"].isin(available_users)
]

recommendations_reduced_df.to_csv("./data/recommendations.csv", index=False)
