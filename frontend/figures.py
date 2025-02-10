import plotly.graph_objects as go
import pandas as pd


def products_evolution_fig(tab):
    arr = tab.set_index('Product').T.reset_index()
    arr.loc[arr['level_0'].duplicated(), 'level_0'] = ''
    def color(i, e):
        if (type(e) == str) or (i == 2) or (i == 3):
            return 'darkslategray'
        else:
            if e > 0:
                return 'rgb(0, 150, 0)'
            elif e < 0:
                return 'rgb(216, 30, 0)'
            else:
                return 'darkslategray'
    colors = [[color(i, e) for i, e in enumerate(l)] for l in arr.T.values]
    cols = arr.columns.tolist()
    cols[0] = ''
    cols[1] = ''
    rowEvenColor = '#e6e6e6'
    rowOddColor = 'white'
    table = go.Table(
        domain=dict(x=[0, 1], y=[0, 1]),
        header=dict(values=cols,
                    fill_color = '#97D700'),
        cells=dict(values=arr.T.values,
                font=dict(color=colors),
                fill_color = [[rowOddColor,rowEvenColor,rowOddColor, rowEvenColor,rowOddColor]*5],
                align = ['left', 'left', 'center']))
    tab_fig = go.Figure(data=table)
    tab_fig.update_layout(    
        margin=dict(
            l=10,
            r=10,
            t=10,
            b=10
        ))
    return tab_fig


def plot_spot_fig(spot_df_dict, tenor_info):
    spot_fig = go.Figure()
    for name in spot_df_dict.keys():
        delta_t = spot_df_dict[name].index[1] - spot_df_dict[name].index[0]
        index = [spot_df_dict[name].index[0]] + [spot_df_dict[name].index[i] for i in range(1, len(spot_df_dict[name].index)) for _ in range(2)] + [spot_df_dict[name].index[-1]+delta_t]
        spot_fig.add_trace(go.Scatter(x=index, 
                                      y=[e for e in spot_df_dict[name].price for _ in range(2)], 
                                      name=name, 
                                      visible=(name == 'Hourly'), 
                                      showlegend=False,
                                      line={'color':'#0057B8'},
                                      hovertemplate="%{y} €/MWh"))
    for tenor in ['Month', 'Quarter', 'Year']:
        df = tenor_info.xs(
            tenor, 
            level='tenor'
            ).reset_index(drop=True).set_index('delivery_start').drop(
                columns='delivery_end'
                ).reindex(
                    spot_df_dict['Daily'].index, 
                    method='ffill').dropna()
        for name in ['Spot_Base', 'Spot_Peak', 'Base', 'Peak']:
            delta_t = df.index[1] - df.index[0]
            index = [df.index[0]] + [df.index[i] for i in range(1, len(df.index)) for _ in range(2)] + [df.index[-1]+delta_t]
            spot_fig.add_trace(go.Scatter(
                x=index, 
                y=[e for e in df[name] for _ in range(2)], 
                visible=False, 
                showlegend=False, 
                legendgroup=tenor, 
                name=f'{tenor} {name}', 
                mode='lines', 
                line=dict(color='#97D700',
                          dash='solid' if 'Spot' in name else 'dot'),
                hovertemplate="%{y} €/MWh"))
    update_layout(spot_fig, 'FR Spot Prices')
    return spot_fig


def plot_futures_fig(futures_df):
    futures_fig = go.Figure()
    for (year, tenor, product_index, type), data in futures_df.groupby(['delivery_year', 'tenor', 'product_index', 'type'], 
                                                                       sort=True):
        futures_fig.add_trace(
            go.Scatter(
                x=data.trading_date,
                y=data.settlement_price,
                name=f'{year}_{tenor[0]}{product_index}_{type}',
                visible=False,
                mode='lines',
                hovertemplate='%{y} €/MWh',
                meta={
                    'year' : int(year),
                    'tenor' : tenor,
                    'product_index' : int(product_index),
                    'type' : type
                }
            )
        )
    update_layout(futures_fig, 'Futures Price Trend')
    futures_fig.update_xaxes(title='Trading Date')
    return futures_fig


def plot_gas_fig(gas_df):
    gas_fig = go.Figure()
    gas_fig.add_trace(go.Scatter(x=gas_df.index, y=gas_df.last_price, line=dict(color='#0057B8')))
    update_layout(gas_fig, 'Gas Spot Prices')
    gas_fig.update_layout(showlegend=False)
    return gas_fig


def plot_pfc_fig(adj_PFC_dict, quot):
    fig = go.Figure()
    for name in adj_PFC_dict.keys():
        summary_df = pd.DataFrame({
            "Median": adj_PFC_dict[name].quantile(0.5, axis=1),
            "Lower": adj_PFC_dict[name].quantile(0.25, axis=1),
            "Upper": adj_PFC_dict[name].quantile(0.75, axis=1)
        })
        delta_t = summary_df.index[1] - summary_df.index[0]
        index = [summary_df.index[0]] + [summary_df.index[i] for i in range(1, len(summary_df.index)) for _ in range(2)] + [summary_df.index[-1]+delta_t]
        fig.add_trace(go.Scatter(x=index,
                                 y=[e for e in summary_df.Median for _ in range(2)],
                                 mode="lines",
                                 name="Median",
                                 legendgroup=name,
                                 line=dict(color="blue"),
                                 visible=(name=='Hourly')))
        fig.add_trace(go.Scatter(x=index + index[::-1],
                                 y=[e for e in summary_df.Lower for _ in range(2)] + [e for e in summary_df.Upper for _ in range(2)][::-1],
                                 fill="toself",
                                 fillcolor="rgba(0, 0, 255, 0.2)",
                                 line=dict(color="rgba(255, 255, 255, 0)"),
                                 name=f'Quantile (1/4-3/4)',
                                 hoverinfo="skip",
                                 legendgroup=name,
                                 visible=(name=='Hourly')))
    df = quot.copy()
    df.loc[df.tenor == 'Quarter', 'start_month'] = (df.loc[df.tenor == 'Quarter', 'product_index']-1)*3+1
    df.loc[df.tenor == 'Year', 'start_month'] = 1
    df.loc[df.tenor == 'Month', 'start_month'] = df.loc[df.tenor == 'Month', 'product_index']
    df.loc[df.tenor == 'Quarter', 'end_month'] = ((df.loc[df.tenor == 'Quarter', 'product_index']-1)*3+1+3-1)%12+1
    df.loc[df.tenor == 'Year', 'end_month'] = 1
    df.loc[df.tenor == 'Month', 'end_month'] = (df.loc[df.tenor == 'Month', 'product_index']+1-1)%12+1
    df.loc[df.tenor == 'Quarter', 'end_year'] = df.loc[df.tenor == 'Quarter', 'delivery_year'] + ((df.loc[df.tenor == 'Quarter', 'product_index']-1)*3+1+3-1)//12
    df.loc[df.tenor == 'Year', 'end_year'] = df.loc[df.tenor == 'Year', 'delivery_year'] + 1
    df.loc[df.tenor == 'Month', 'end_year'] = df.loc[df.tenor == 'Month', 'delivery_year'] + (df.loc[df.tenor == 'Month', 'product_index']+1-1)//12
    df.start_month = df.start_month.astype(int).astype(str).str.zfill(2)
    df.end_month = df.end_month.astype(int).astype(str).str.zfill(2)
    df.end_year = df.end_year.astype(int)
    df['start_date'] = pd.to_datetime(df.delivery_year.astype(str) + '-' + df.start_month + '-01T00:00:00')
    df['end_date'] = pd.to_datetime(df.end_year.astype(str) + '-' + df.end_month + '-01T00:00:00')

    for (tenor, type), sub_df in df.groupby(['tenor', 'type']):
        sub_df.sort_values(by='start_date', inplace=True)
        delta_t = summary_df.index[1] - summary_df.index[0]
        index = [sub_df['start_date'].iloc[i] if b else sub_df['end_date'].iloc[i] for i in range(len(sub_df)) for b in [True, False]]
        vals = [e for e in sub_df['settlement_price'] for _ in range(2)]
        fig.add_trace(go.Scatter(x=index,
                                 y=vals,
                                 mode="lines",
                                 name=f'{tenor} {type}',
                                 legendgroup=tenor,
                                 line=dict(color="#97D700"),
                                 visible=(tenor=='Quarter')))
    update_layout(fig, 'PFC')
    return fig


def update_layout(fig, title):
    fig.update_layout(
        plot_bgcolor='white',
        hovermode='x unified',
        title=title
    )
    fig.update_xaxes(
        title=None,
        ticks='outside',
        showline=True,
        linecolor='black',
        gridcolor=None,
        ticklabelmode='period'
    )
    fig.update_yaxes(
        title='Price (€/MWh)',
        ticks='outside',
        showline=True,
        linecolor='black',
        gridcolor='lightgrey',
    )