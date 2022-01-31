import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from scipy import stats

st.set_page_config(layout="wide")

@st.cache
def load_data():
    # Read data from your bucket
    df = pd.read_csv('gs://simulate-dataflow/conversion.csv')
    df.set_index('Unnamed: 0', inplace=True)

    return df

def main():
    # Load data
    df = load_data()

    # Prepare data
    total_visitor_a = df['design_a'].loc[['total']][0]
    a_conversion = df['design_a'].loc[['conversion']][0]
    total_visitor_b = df['design_b'].loc[['total']][0]
    b_conversion = df['design_b'].loc[['conversion']][0]


    # If samples = N and conversion = X, then X follows a binomial distribution. 
    # If we choose beta distribution for the prior, the posterior will also be beta distribution too
    # The Posterior beta distribution will be Beta(alpha + X, beta + N - X)
    alpha_prior = 1
    beta_prior = 1

    samples = 20000
    samples_posterior_a = stats.beta.rvs(alpha_prior + total_visitor_a, beta_prior + total_visitor_a - a_conversion, size=samples)
    samples_posterior_b = stats.beta.rvs(alpha_prior + total_visitor_b, beta_prior + total_visitor_b - b_conversion, size=samples)

    prob = (samples_posterior_a < samples_posterior_b).mean()
    
    # Display table
    # st.table(df)

    # Graph setting
    fig = plt.figure(figsize=(20,10))
    ax = fig.add_subplot(111)
    sns.distplot(samples_posterior_a, ax=ax, label='CVR of A')
    sns.distplot(samples_posterior_b, ax=ax, label='CVR of B')
    ax.set_ylabel('KDE', fontsize='xx-large')
    ax.set_xlabel('CVR', fontsize='xx-large')
    ax.set_title('CVR Distributions', fontsize='xx-large')
    ax.legend(loc='upper right', fontsize='xx-large')
    fig.tight_layout()

    # Display a graph
    # st.subheader('The CVR Distributions')
    # st.pyplot(fig)

    container = st.container()
    with container:
        st.title('The result of the bayesian A/B test')
        col1, col2 = st.columns(2)

        with col1:
            st.subheader('Data Table')
            st.table(df)
        with col2:
            st.subheader('The CVR Distributions')
            st.pyplot(fig)
    
    st.markdown(fr'''
                <center><font size=5>The probability of CVR A being smaller than CVR B: {"{:.1%}".format(prob)}</font></center>
                ''',
                unsafe_allow_html=True
            )

    
            

if __name__ == "__main__":
    main()