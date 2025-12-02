import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from prefect import flow, task
from prefect_slack import SlackWebhook  # requires prefect-slack installed

# -----------------------------
# Load dataframe (adapt as needed)
# -----------------------------
@task
def load_df():
    # TODO: replace with your real loading method
    df = pd.read_csv("data.csv")

    df["created_at"] = pd.to_datetime(df["created_at"], utc=True)
    return df


# -----------------------------
# Create the ERROR bar chart image
# -----------------------------
@task
def create_error_chart(df):
    now = pd.Timestamp.utcnow()
    filtered_df = df[df["created_at"] >= now - pd.Timedelta(hours=24)]
    
    error_df = filtered_df[filtered_df["ats_sync_status"] == "error"]
    grouped_df = (
        error_df.groupby("company_name").size()
        .reset_index(name="error_count")
        .sort_values(by="error_count", ascending=True)
    )

    plt.figure(figsize=(10, 6))
    ax = sns.barplot(
        x="error_count",
        y="company_name",
        data=grouped_df,
        palette="Reds_r"
    )

    plt.title("ATS Sync Errors in the Last 24 Hours")
    ax.set_xlabel("")
    ax.set_ylabel("")

    for index, value in enumerate(grouped_df["error_count"]):
        ax.text(
            value + 0.5, 
            index, 
            str(value), 
            va="center", 
            ha="left", 
            fontsize=12, 
            fontweight="bold"
        )

    output_path = "ats_sync_errors.png"
    plt.tight_layout()
    plt.savefig(output_path, dpi=200)
    plt.close()

    return output_path


# -----------------------------
# Send PNG to Slack
# -----------------------------
@task
def send_to_slack(image_path):
    slack = SlackWebhook.load("ats-report-slack")  # Prefect Slack block you create
    slack.notify(
        text="ATS Sync Errors in Last 24 Hours 📊",
        blocks=None,
        file=image_path,  # <-- upload the PNG
    )


# -----------------------------
# MAIN FLOW
# -----------------------------
@flow
def ats_sync_error_report_flow():
    df = load_df()
    image_path = create_error_chart(df)
    send_to_slack(image_path)


if __name__ == "__main__":
    ats_sync_error_report_flow()
