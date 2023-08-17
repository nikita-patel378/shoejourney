import psycopg2
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt


def fetch_and_aggregate_data():
    # Database connection details
    conn = psycopg2.connect(
        host="localhost",
        user="####",
        password="####",
        database="####",
        port="####"
    )

    # Query to join shoes and activities and get total distance run by each shoe
    query = """
    SELECT 
        s.shoeid, 
        s.brand, 
        s.model,
        s.color,
        s.isretired,
        COALESCE(SUM(a.distance), 0) as distance
    FROM 
        shoes s
    LEFT JOIN 
        activities a ON s.shoeid = a.shoeid
    GROUP BY 
        s.shoeid, s.brand, s.model, s.color, s.isretired
    ORDER BY 
        distance DESC;
    """

    # Use pandas to directly read the result into a DataFrame
    merged_data = pd.read_sql_query(query, conn)

    # Close the connection
    conn.close()

    return merged_data


# Execute the function and print results
result = fetch_and_aggregate_data()


def visualize_with_seaborn(data):
    # Create a unique identifier for each shoe model by combining model and shoeid
    data['unique_model'] = data['model'] + " (" + data['shoeid'].astype(str) + ")"

    # Set the figure size and background color
    fig = plt.figure(figsize=(12, 6), facecolor='#EBB9DF')

    # Create a bar plot using the unique_model column
    ax = sns.barplot(x='unique_model', y='distance', hue='isretired', data=data, palette={True: '#F19953', False: '#AAFAC8'})

    # Set the title, labels, and background color
    ax.set_title('Total Miles by Shoe with Retirement Status')
    ax.set_xlabel('Shoe Model')
    ax.set_ylabel('Total Distance (miles)')
    ax.legend(title='Is Retired?')
    plt.xticks(rotation=45)  # Rotate x-axis labels for better visibility

    # Set the background color for the plotting area
    ax.set_facecolor('#392F5A')

    # Display the plot
    plt.show()


visualize_with_seaborn(result)

result.drop(columns=['unique_model'], inplace=True)
def highlight_retired_rows(row):
    if row['isretired']:
        return ['background-color: #F5D5ED'] * len(row)
    return [''] * len(row)


# Style the rows with IsRetired=True
styled_result = result.style.apply(highlight_retired_rows, axis=1)

styled_result.set_caption("TOTAL NUMBER OF MILES WITH EACH SHOE").format_index(str.upper, axis=1)


