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
        spot_fig.add_trace(go.Scatter(x=spot_df_dict[name].index, 
                                      y=spot_df_dict[name].price, 
                                      name=name, 
                                      visible=(name == 'Hourly'), 
                                      showlegend=False,
                                      line={'color':'#0057B8', 'shape':'hv'},
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
            spot_fig.add_trace(go.Scatter(
                x=df.index, 
                y=df[name], 
                visible=False, 
                showlegend=False, 
                legendgroup=tenor, 
                name=f'{tenor} {name}', 
                mode='lines', 
                line=dict(color='#97D700',
                          dash='solid' if 'Spot' in name else 'dot',
                          shape='hv'),
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


def plot_pfc_fig(adj_PFC):
    summary_df = pd.DataFrame({
        "Mean": adj_PFC.mean(axis=1),
        "Lower": adj_PFC.quantile(0.25, axis=1),
        "Upper": adj_PFC.quantile(0.75, axis=1)
    })
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=summary_df.index,
                             y=summary_df.Mean,
                             mode="lines",
                             name="Mean",
                             line=dict(color="blue")))
    fig.add_trace(go.Scatter(x=summary_df.index.tolist() + summary_df.index.tolist()[::-1],
                             y=summary_df.Lower.tolist() + summary_df.Upper.tolist()[::-1],
                             fill="toself",
                             fillcolor="rgba(0, 0, 255, 0.2)",
                             line=dict(color="rgba(255, 255, 255, 0)"),
                             hoverinfo="skip",
                             name="Envelope (0.25-0.75)"))
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