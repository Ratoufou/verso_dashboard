import pandas as pd 
import psycopg2
import plotly.graph_objects as go
import datetime as dt
from dash import Dash, dcc, html, Input, Output, callback, ctx, dash_table

DB_CONFIG = {
    'host' : 'db',
    'database' : 'verso_database',
    'user' : 'root',
    'password' : 'versosql'
}

## DA Prices

conn = psycopg2.connect(**DB_CONFIG) 
cursor = conn.cursor()
cursor.execute('SELECT * FROM elec_day_ahead_market')
results = cursor.fetchall()
spot_df = pd.DataFrame(data=results, columns=['start', 'end', 'source', 'country', 'tenor', 'price', 'currency']).sort_values(by='start')
spot_df.price = spot_df.price.astype('float')
spot_df['start'] = pd.to_datetime(spot_df.start, utc=True).dt.tz_convert('CET').dt.tz_localize(None)
spot_df['end'] = pd.to_datetime(spot_df.end, utc=True).dt.tz_convert('CET').dt.tz_localize(None)

## Futures Prices

conn = psycopg2.connect(**DB_CONFIG) 
cursor = conn.cursor()
cursor.execute('SELECT * FROM elec_futures_market')
results = cursor.fetchall()
futures_df = pd.DataFrame(data=results, columns=['trading_date', 'delivery_start', 'delivery_end', 'source', 'country', 'peak', 'tenor', 'price', 'currency'])
futures_df.trading_date = pd.to_datetime(futures_df.trading_date).dt.date
futures_df.delivery_start = pd.to_datetime(futures_df.delivery_start)
futures_df.delivery_end = pd.to_datetime(futures_df.delivery_end)
futures_df['middle'] = futures_df.delivery_start + (futures_df.delivery_end - futures_df.delivery_start)/2 
futures_df.delivery_start = futures_df.delivery_start.dt.date 
futures_df.delivery_end = futures_df.delivery_end.dt.date
futures_df['Year'] = futures_df.middle.dt.year
futures_df['Quarter'] = futures_df.middle.dt.quarter
futures_df['Month'] = futures_df.middle.dt.month

tenor_df = futures_df[futures_df.tenor.isin(['Year', 'Quarter', 'Month'])][['trading_date', 'peak', 'tenor', 'price', 'Year', 'Quarter', 'Month']].copy()
def product(x):
    if x.tenor == 'Year':
        return f'Cal-{x.Year % 100}'
    elif x.tenor == 'Quarter':
        return f'Q{x.Quarter}/{x.Year % 100}'
    elif x.tenor == 'Month':
        return f'{dt.date(year=x.Year, month=x.Month, day=1).strftime(format="%b")}/{x.Year % 100}'
    else:
        return pd.NA
tenor_df['Product'] = tenor_df.apply(product, axis=1)
tenor_df = tenor_df.dropna().sort_values(['tenor', 'Year', 'Quarter', 'Month', 'trading_date']).reset_index(drop=True)

## Gas Prices 

conn = psycopg2.connect(**DB_CONFIG) 
cursor = conn.cursor()
cursor.execute('SELECT * FROM gas_day_ahead_market')
results = cursor.fetchall()
gas_df = pd.DataFrame(data=results, columns=['trading_date', 'delivery_start', 'delivery_end', 'source', 'country', 'tenor', 'price', 'currency'])
gas_df.price = spot_df.price.astype('float')

## Plots 

pandas_resample_keyword = {
    'Day' : 'D',
    'Month' : 'MS',
    'Quarter' : 'QS',
    'Year' : 'YS'
}

def last_price(tenor, x):
    if tenor == 'Year':
        last_date = dt.date(year=x.name[0], month=1, day=1)
    elif tenor == 'Month':
        last_date = dt.date(year=x.name[0], month=x.name[1], day=1)
    elif tenor == 'Quarter':
        last_date = dt.date(year=x.name[0], month=(x.name[1]-1)*3+1, day=1)
    else:
        raise Exception()
    filtered_df = x[x.trading_date < last_date]
    try:
        # val = filtered_df.sort_values(by='trading_date').iloc[-1][['price']]
        val = filtered_df[['price']].mean()
    except IndexError:
        val = pd.Series(data=[pd.NA], index=['price'])
    return val

def gather_tenor_info(spot_df, futures_df, tenor):
    tenor_info = ['Year'] if tenor == 'Year' else ['Year', tenor]
    tenor_df = futures_df[futures_df.tenor == tenor][['trading_date', 'peak', 'price'] + tenor_info]
    last_quotation_df = tenor_df.groupby(tenor_info + ['peak']).apply(lambda x: last_price(tenor, x), include_groups=False).reset_index()
    last_quotation_peak_df = last_quotation_df[last_quotation_df.peak][tenor_info + ['price']]
    last_quotation_base_df = last_quotation_df[~last_quotation_df.peak][tenor_info + ['price']]
    last_quotation_base_and_peak_df = pd.merge(left=last_quotation_base_df, right=last_quotation_peak_df, on=tenor_info, suffixes=('_base', '_peak'))

    resampled_df = spot_df[['start', 'end', 'price']].resample(pandas_resample_keyword[tenor], on='start').agg({'end' : 'max', 'price' : 'mean'}).reset_index()
    resampled_df['middle'] = resampled_df.start + (resampled_df.end - resampled_df.start)/2 
    resampled_df['Year'] = resampled_df.middle.dt.year
    resampled_df['Quarter'] = resampled_df.middle.dt.quarter
    resampled_df['Month'] = resampled_df.middle.dt.month

    tenor_summary_df = pd.merge(left=last_quotation_base_and_peak_df, 
                                right=resampled_df, 
                                on=tenor_info, 
                                how='inner')[['start', 'end', 'price', 'price_base', 'price_peak']]
    return tenor_summary_df

def color_gradient(color1, color2, n):
    return [tuple([int(color1[j] + (color2[j] - color1[j])*i/(n-1)) for j in range(3)]) for i in range(n)]

today = futures_df.trading_date.max()
this_week_week = today.isocalendar().week
this_week_year = today.isocalendar().year
previous_week_week = (today - dt.timedelta(days=7)).isocalendar().week
previous_week_year = (today - dt.timedelta(days=7)).isocalendar().year
quotations = futures_df.copy()
quotations['trading_week'] = quotations.trading_date.apply(lambda x: x.isocalendar().week)  
quotations['trading_year'] = quotations.trading_date.apply(lambda x: x.isocalendar().year) 
this_week_quotations = quotations[(quotations.trading_year == this_week_year) & (quotations.trading_week == this_week_week)]
previous_week_quotations = quotations[(quotations.trading_year == previous_week_year) & (quotations.trading_week == previous_week_week)]

liquidity_dict = {
    'Month' : 3,
    'Quarter' : 4,
    'Year' : 3
}
df_ls = []
for tenor, liquidity in liquidity_dict.items():
    for i in range(1, liquidity+1):
        if tenor == 'Month':
            delivery_month = (today.month + i - 1)%12 +1
            delivery_year = today.year + (today.month + i - 1)//12
            this_week_df = this_week_quotations[(this_week_quotations.tenor == 'Month') & 
                                                (this_week_quotations.Year == delivery_year) & 
                                                (this_week_quotations.Month == delivery_month)][['peak', 'price']].groupby('peak').mean()
            previous_week_df = previous_week_quotations[(previous_week_quotations.tenor == 'Month') & 
                                                        (previous_week_quotations.Year == delivery_year) & 
                                                        (previous_week_quotations.Month == delivery_month)][['peak', 'price']].groupby('peak').mean()
        elif tenor == 'Quarter':
            today_quarter = ((today.month-1) // 3) + 1
            delivery_quarter = (today_quarter + i - 1)%4 + 1
            delivery_year = today.year + (today_quarter + i - 1)//4
            this_week_df = this_week_quotations[(this_week_quotations.tenor == 'Quarter') & 
                                                (this_week_quotations.Year == delivery_year) & 
                                                (this_week_quotations.Quarter == delivery_quarter)][['peak', 'price']].groupby('peak').mean()
            previous_week_df = previous_week_quotations[(previous_week_quotations.tenor == 'Quarter') & 
                                                        (previous_week_quotations.Year == delivery_year) & 
                                                        (previous_week_quotations.Quarter == delivery_quarter)][['peak', 'price']].groupby('peak').mean()
        elif tenor == 'Year':
            delivery_year = today.year + i
            this_week_df = this_week_quotations[(this_week_quotations.tenor == 'Year') & 
                                                (this_week_quotations.Year == delivery_year)][['peak', 'price']].groupby('peak').mean()
            previous_week_df = previous_week_quotations[(previous_week_quotations.tenor == 'Year') & 
                                                        (previous_week_quotations.Year == delivery_year)][['peak', 'price']].groupby('peak').mean()

            
        df = ((this_week_df - previous_week_df)*100 / previous_week_df).rename(columns={'price' : 'evolution'})
        df = pd.concat([this_week_df, df], axis=1).reset_index()
        df['Product'] = f'{tenor[0]}+{i}'
        df_ls.append(df)
tot_df = pd.concat(df_ls, ignore_index=True)
tab = tot_df.groupby('Product').apply(lambda x: pd.Series([x[~x.peak]['price'].iloc[0], 
                                                           x[~x.peak]['evolution'].iloc[0],
                                                           x[x.peak]['price'].iloc[0],
                                                           x[x.peak]['evolution'].iloc[0]], 
                                                          index=pd.MultiIndex.from_tuples([('Base', 'Price (€)'), 
                                                                                           ('Base', '% Change'), 
                                                                                           ('Peak', 'Price (€)'), 
                                                                                           ('Peak', '% Change')])), 
                                      include_groups=False).round(1).reset_index()

def generate_table(tab):
    col_to_id = {index: str(i) for i, index in enumerate(tab.columns)}
    cols = [{'name' : list(col), 'id' : i} for col, i in col_to_id.items()]
    data = [
        {col_to_id[col] : v
        for col, v in record_dict.items()}
        for record_dict in tab.to_dict('records')
    ]
    return dash_table.DataTable(columns=cols, 
                                data=data,
                                cell_selectable=False,
                                merge_duplicate_headers=True,
                                style_as_list_view=True,
                                style_cell={'textAlign' : 'left'},
                                style_data_conditional=(
                                    [{'if': {
                                            'filter_query': '{{{}}} > {}'.format(col, 0),
                                            'column_id': col
                                            },
                                        'color': 'green'
                                        } for col in ['2', '4']
                                    ] +
                                    [{'if': {
                                            'filter_query': '{{{}}} < {}'.format(col, 0),
                                            'column_id': col
                                            },
                                        'color': 'red'
                                        } for col in ['2', '4']
                                    ]
                                ))

app = Dash(__name__)

# Spot Figure
spot_df_dict = {'Hourly' : spot_df.set_index('start')[['price']]}
spot_df_dict['Daily'] = spot_df_dict['Hourly'].resample('D').mean().round(1)
spot_df_dict['Monthly'] = spot_df_dict['Hourly'].resample('MS').mean().round(1)

spot_fig = go.Figure()
for name in spot_df_dict.keys():
    if name == 'Monthly':
        spot_fig.add_trace(go.Scatter(x=spot_df_dict[name].index, y=spot_df_dict[name].price, 
                                      name=name, visible=(name == 'Hourly'), showlegend=False,
                                      # xperiod='M1', xperiodalignment='middle', 
                                      line={'color':'#0057B8', 'shape':'hv'},
                                      hovertemplate="%{y} €"))
    else:
        spot_fig.add_trace(go.Scatter(x=spot_df_dict[name].index, y=spot_df_dict[name].price, 
                                      name=name, visible=(name == 'Hourly'), showlegend=False,
                                      line={'color':'#0057B8', 'shape':'hv'},
                                      hovertemplate="%{y} €"))

for tenor in ['Month', 'Quarter', 'Year']:
    tenor_summary_df = gather_tenor_info(spot_df, futures_df, tenor).set_index('start').reindex(spot_df_dict['Daily'].index, method='ffill').dropna()
    tenor_summary_df = tenor_summary_df[['price', 'price_base', 'price_peak']].round(2)
    spot_fig.add_trace(go.Scatter(x=tenor_summary_df.index, y=tenor_summary_df.price, visible=False, showlegend=False, 
                                  legendgroup=tenor, name=f'{tenor} Spot', mode='lines', line=dict(color='#97D700'),
                                 hovertemplate="%{y} €"))
    spot_fig.add_trace(go.Scatter(x=tenor_summary_df.index, y=tenor_summary_df.price_base, visible=False, showlegend=False, 
                                  legendgroup=tenor, name=f'{tenor} Base', mode='lines', line=dict(color='#97D700', dash='dot'),
                                  hovertemplate="%{y} €"))   
    spot_fig.add_trace(go.Scatter(x=tenor_summary_df.index, y=tenor_summary_df.price_peak, visible=False, showlegend=False, 
                                  legendgroup=tenor, name=f'{tenor} Peak', mode='lines', line=dict(color='#97D700', dash='dot'),
                                  hovertemplate="%{y} €"))   

spot_fig.update_layout(
    plot_bgcolor='white',
    hovermode='x unified',
    title='FR Spot & Futures* Prices'
)
spot_fig.update_xaxes(
    title=None,
    ticks='outside',
    ticklabelmode='period',
    showline=True,
    linecolor='black',
    gridcolor=None,
)
spot_fig.update_yaxes(
    title='Price (€)',
    ticks='outside',
    showline=True,
    linecolor='black',
    gridcolor='lightgrey',
)

futures_fig = go.Figure()
for (product, peak), data in tenor_df.groupby(['Product', 'peak'], sort=False):
    futures_fig.add_trace(go.Scatter(x=data.trading_date, 
                                     y=data.price,
                                     name= product,
                                     visible=False,
                                     mode='lines',
                                     hovertemplate="%{y} €",
                                     meta={'tenor' : data.tenor.iloc[0],
                                           'year' : int(data.Year.iloc[0]),
                                           'quarter' : int(data.Quarter.iloc[0]),
                                           'month' : int(data.Month.iloc[0]),
                                           'peak' : bool(peak)}
                                    ))
indices_to_marks = {i: int(e) for i, e in enumerate(sorted(list(tenor_df.Year.unique())))}
marks_to_indices = {v: k for k, v in indices_to_marks.items()}
# futures_fig.update_traces(
#     hovertemplate="<b>%{customdata[0]}</b> - %{y} €<extra></extra>"
# )
futures_fig.update_layout(
    plot_bgcolor='white',
    hovermode='x unified',
    title='Futures Prices Trend'
)
futures_fig.update_xaxes(
    title='Trading Date',
    ticks='outside',
    showline=True,
    linecolor='black',
    gridcolor=None,
)
futures_fig.update_yaxes(
    title='Price (€)',
    ticks='outside',
    showline=True,
    linecolor='black',
    gridcolor='lightgrey',
)

gas_df_clean = gas_df[gas_df.tenor == 'Day'][['delivery_start', 'price']].set_index('delivery_start').sort_index()
gas_fig = go.Figure()
gas_fig.add_trace(go.Scatter(x=gas_df_clean.index, y=gas_df_clean.price, line=dict(color='#0057B8')))
gas_fig.update_layout(
    plot_bgcolor='white',
    showlegend=False,
    title='Gas Spot Prices'
)
gas_fig.update_xaxes(
    title=None,
    ticks='outside',
    showline=True,
    linecolor='black',
    gridcolor=None,
    ticklabelmode="period"
)
gas_fig.update_yaxes(
    title='Price (€)',
    ticks='outside',
    showline=True,
    linecolor='black',
    gridcolor='lightgrey',
)

app.layout = html.Div([
    html.Link(
            href="https://fonts.googleapis.com/css2?family=Open+Sans:wght@400;600&display=swap",
            rel="stylesheet"
    ),
    html.H1(
        children=f'Verso Energy dashboard - {dt.date.today().strftime(format="%d/%m/%Y")}',
        style={'textAlign' : 'center',
               'background' : 'linear-gradient(to right, #97D700, #0057B8)',
               'color' : 'white',
               'width' : '100%'},
    ),
    html.H2(
        children='Power',
        style={'background' : 'linear-gradient(to right, #97D700, #0057B8)',
               'color' : 'white',
               'width' : '100%'},
    ),
    html.Div([
        html.Div([
            html.Div([
                dcc.DatePickerRange(
                    id='date-picker-range',
                    display_format='D-M-Y',
                    min_date_allowed=spot_df_dict['Hourly'].index[0].date(),
                    max_date_allowed=spot_df_dict['Hourly'].index[-1].date(),
                    start_date = dt.date(year=spot_df_dict['Hourly'].index[-1].date().year, month=spot_df_dict['Hourly'].index[-1].date().month, day=1),
                    end_date = dt.date(year=spot_df_dict['Hourly'].index[-1].date().year, month=spot_df_dict['Hourly'].index[-1].date().month, day=spot_df_dict['Hourly'].index[-1].date().day)),
                html.Button(
                    'YTD',
                    id='ytd-button',
                    style={'height':'36px', 'width' : '50px'}),
                html.Button(
                    'QTD',
                    id='qtd-button',
                    style={'height':'36px', 'width' : '50px'}),
                html.Button(
                    'MTD',
                    id='mtd-button',
                    style={'height':'36px', 'width' : '50px'}),
                html.Button(
                    'All',
                    id='all-button',
                    style={'height':'36px', 'width' : '50px'}),
                dcc.Dropdown(
                    ['Hourly', 'Daily', 'Monthly'],
                    'Hourly',
                    id='sampling-dropdown',
                    clearable=False,
                    searchable=False,
                    style={'width':'100px', 'height':'36px'}
                ),
                dcc.Dropdown(
                    ['None', 'Month', 'Quarter', 'Year'],
                    'None',
                    id='tenor-dropdown',
                    clearable=False,
                    searchable=False,
                    style={'width':'100px', 'height':'36px'}
                ),
                html.Button(
                    'Download',
                    id='download-button',
                    style={'height':'36px', 'width' : '70px'}),
                dcc.Download(
                    id='download-spot'
                )
            ], style={'width': '100%', 
                      'display': 'flex', 
                      'justify-content': 'space-evenly',
                      'align-items':'center'}),
            dcc.Graph(
                id='graph',
                figure=spot_fig)
        ], style={'width':'50%'}),
        html.Div([
            html.Div([
                dcc.DatePickerRange(
                    id='futures-fig-dates-picker',
                    display_format='D-M-Y',
                    min_date_allowed=tenor_df.trading_date.min(),
                    max_date_allowed=tenor_df.trading_date.max(),
                    start_date = tenor_df.trading_date.min(),
                    end_date = tenor_df.trading_date.max()
                ),
                html.Button(
                    'YTD',
                    id='ytd-button-futures',
                    style={'height':'36px', 'width' : '50px'}),
                html.Button(
                    'QTD',
                    id='qtd-button-futures',
                    style={'height':'36px', 'width' : '50px'}),
                html.Button(
                    'MTD',
                    id='mtd-button-futures',
                    style={'height':'36px', 'width' : '50px'}),
                html.Button(
                    'All',
                    id='all-button-futures',
                    style={'height':'36px', 'width' : '50px'}),
                dcc.Dropdown(
                    ['Year', 'Quarter', 'Month'],
                    'Year',
                    id='futures-fig-tenor-dropdown',
                    clearable=False,
                    searchable=False,
                    style={'width':'100px', 'height':'36px'}),
                dcc.Dropdown(
                    ['Base', 'Peak'],
                    'Base',
                    id='futures-fig-peak-dropdown',
                    clearable=False,
                    searchable=False,
                    style={'width':'100px', 'height':'36px'}),
                html.Button(
                    'Download',
                    id='download-button-futures',
                    style={'height':'36px', 'width' : '70px'}),
                dcc.Download(
                    id='download-futures')
            ], style={'width':'100%',
                      'display':'flex',
                      'justify-content': 'space-evenly',
                      'align-items':'center'}),
            dcc.Graph(
                id='futures-graph',
                figure=futures_fig), 
            dcc.RangeSlider(
                id='yearslider',
                min=0, 
                max=len(indices_to_marks)-1, 
                marks=indices_to_marks, 
                value=[marks_to_indices[today.year]+1, marks_to_indices[today.year]+3], 
                updatemode='mouseup',
                step=None,
                allowCross=False
            ),            
        ], style={'width':'50%'}),

    ], style={'display': 'flex',
              'justify-content': 'flex-start',
              'width': '100%'}),
    html.H3([
        'Futures Prices and Trends**']),
    html.Div([
        generate_table(tab)
    ], style={'width':'70%'}),
    html.Footer(
        '* For futures prices, the value used is the average of all prices at which a trade has taken place (without weighting).',
        style={'align-self':'flex-start'}
    ),
    html.Footer(
         '** For prices and trends, average quotations for the current and previous weeks are taken into account.',
        style={'align-self':'flex-start'}
    ),
    html.H2(
        children='Gas',
        style={'background' : 'linear-gradient(to right, #97D700, #0057B8)',
               'color' : 'white',
               'width' : '100%'},
    ),
    html.Div([
        html.Div([
            dcc.DatePickerRange(
                id='date-picker-range-gas',
                display_format='D-M-Y',
                min_date_allowed=gas_df_clean.index[0].date(),
                max_date_allowed=gas_df_clean.index[-1].date(),
                start_date = dt.date(year=gas_df_clean.index[-1].date().year, month=gas_df_clean.index[-1].date().month, day=1),
                end_date = dt.date(year=gas_df_clean.index[-1].date().year, month=gas_df_clean.index[-1].date().month, day=gas_df_clean.index[-1].date().day)
            ),
            html.Button(
                'YTD',
                id='ytd-button-gas',
                style={'height':'36px', 'width' : '50px'}),
            html.Button(
                'QTD',
                id='qtd-button-gas',
                style={'height':'36px', 'width' : '50px'}),
            html.Button(
                'MTD',
                id='mtd-button-gas',
                style={'height':'36px', 'width' : '50px'}),
            html.Button(
                'All',
                id='all-button-gas',
                style={'height':'36px', 'width' : '50px'}),
            html.Button(
                'Download',
                id='download-button-gas',
                style={'height':'36px', 'width' : '70px'}),
            dcc.Download(
                id='download-gas')
        ], style={'width': '100%', 
                  'display': 'flex', 
                  'justify-content': 'space-evenly'}),
        dcc.Graph(
            id='gas-graph',
            figure=gas_fig
        )
    ], style={'width':'50%'})   
], style={'height' : '100%',
          'width' : '100%',
          'fontFamily': 'Open Sans',
          'display':'flex',
          'flex-direction':'column',
          'align-items':'center'})

@callback(
    Output('download-spot', 'data'),
    Input('download-button', 'n_clicks'),
    Input('date-picker-range', 'start_date'),
    Input('date-picker-range', 'end_date'),
    Input('sampling-dropdown', 'value'))
def download_spot(btn, start_date, end_date, sampling):
    if 'download-button' == ctx.triggered_id:
        start, end = pd.Timestamp(start_date), pd.Timestamp(end_date) + pd.Timedelta(hours=23)
        filtered_df = spot_df_dict[sampling][start:end]
        return dcc.send_data_frame(filtered_df.to_excel, 
                                   f"{start_date.replace('-', '')}_{end_date.replace('-', '')}_spot_prices.xlsx", 
                                   sheet_name=f'{sampling} Spot Prices')

@callback(
    Output('date-picker-range', 'start_date'),
    Output('date-picker-range', 'end_date'),
    Input('ytd-button', 'n_clicks'),
    Input('qtd-button', 'n_clicks'),
    Input('mtd-button', 'n_clicks'),
    Input('all-button', 'n_clicks'),
    Input('date-picker-range', 'start_date'),
    Input('date-picker-range', 'end_date'),
)
def update_spot_picker_range(ytd, qtd, mtd, all, start_date, end_date):
    last_timestamp = spot_df_dict['Hourly'].index[-1]
    if 'ytd-button' == ctx.triggered_id:
        return dt.date(year=last_timestamp.year, month=1, day=1), last_timestamp.date()
    elif 'qtd-button' == ctx.triggered_id:
        return dt.date(year=last_timestamp.year, month=(last_timestamp.quarter-1)*3 +1, day=1), last_timestamp.date()
    elif 'mtd-button' == ctx.triggered_id:
        return dt.date(year=last_timestamp.year, month=last_timestamp.month, day=1), last_timestamp.date()
    elif 'all-button' == ctx.triggered_id:
        return spot_df_dict['Hourly'].index[0].date(), last_timestamp.date()
    else:
        return start_date, end_date
    
@callback(
    Output('graph', 'figure'),
    Input('date-picker-range', 'start_date'),
    Input('date-picker-range', 'end_date'),
    Input('sampling-dropdown', 'value'),
    Input('tenor-dropdown', 'value'))
def update_spot_figure(start_date, end_date, sampling, tenor):
    start, end = pd.Timestamp(start_date), pd.Timestamp(end_date) + pd.Timedelta(hours=23)
    filtered_df = spot_df_dict[sampling][start:end]
    spot_fig.update_layout(xaxis=dict(range=[filtered_df.index[0], filtered_df.index[-1]]))
    spot_fig.update_layout(yaxis=dict(range=[filtered_df['price'].min()-10, filtered_df['price'].max()+10]))
    for trace in spot_fig.data:
        if (trace.name == sampling) or (trace.legendgroup == tenor):
            trace.visible = True
        else:
            trace.visible = False
    return spot_fig

@callback(
    Output('download-futures', 'data'),
    Input('download-button-futures', 'n_clicks'),
    Input('futures-fig-tenor-dropdown', 'value'),
    Input('futures-fig-peak-dropdown', 'value'),
    Input('yearslider', 'value'),
    Input('futures-fig-dates-picker', 'start_date'),
    Input('futures-fig-dates-picker', 'end_date'))
def download_futures(btn, tenor, peak, value, start_date, end_date):
    if 'download-button-futures' == ctx.triggered_id:
        years = sorted([indices_to_marks[i] for i in range(value[0], value[1]+1)])
        filtered_df = tenor_df[(tenor_df.tenor == tenor) &
                                (tenor_df.peak == (peak == 'Peak')) & 
                                (tenor_df.Year.isin(years)) &
                                (tenor_df.trading_date >= dt.date.fromisoformat(start_date)) &
                                (tenor_df.trading_date <= dt.date.fromisoformat(end_date))][['trading_date', 
                                                                                             'tenor', 
                                                                                             'Product', 
                                                                                             'peak', 
                                                                                             'price']]
        return dcc.send_data_frame(filtered_df.to_excel, 
                                   f"{start_date.replace('-', '')}_{end_date.replace('-', '')}_futures_prices.xlsx", 
                                   sheet_name=f'{tenor} Futures Prices',
                                   index=False)

@callback(
    Output('futures-fig-dates-picker', 'start_date'),
    Output('futures-fig-dates-picker', 'end_date'),
    Input('ytd-button-futures', 'n_clicks'),
    Input('qtd-button-futures', 'n_clicks'),
    Input('mtd-button-futures', 'n_clicks'),
    Input('all-button-futures', 'n_clicks'),
    Input('futures-fig-dates-picker', 'start_date'),
    Input('futures-fig-dates-picker', 'end_date'),
)
def update_futures_picker_range(ytd, qtd, mtd, all, start_date, end_date):
    if 'ytd-button-futures' == ctx.triggered_id:
        return dt.date(year=today.year, month=1, day=1), today
    elif 'qtd-button-futures' == ctx.triggered_id:
        return dt.date(year=today.year, month=((today.month-1)//3)*3 +1, day=1), today
    elif 'mtd-button-futures' == ctx.triggered_id:
        return dt.date(year=today.year, month=today.month, day=1), today
    elif 'all-button-futures' == ctx.triggered_id:
        return tenor_df.trading_date.min(), today
    else:
        return start_date, end_date
    
@callback(
    Output('futures-graph', 'figure'),
    Input('futures-fig-tenor-dropdown', 'value'),
    Input('futures-fig-peak-dropdown', 'value'),
    Input('futures-fig-dates-picker', 'start_date'),
    Input('futures-fig-dates-picker', 'end_date'),
    Input('yearslider', 'value'))
def update_futures_fig(tenor, peak, start_date, end_date, value):
    years = sorted([indices_to_marks[i] for i in range(value[0], value[1]+1)])
    filtered_df = tenor_df[(tenor_df.tenor == tenor) & 
                           (tenor_df.peak == (peak == 'Peak')) & 
                           (tenor_df.trading_date >= dt.date.fromisoformat(start_date)) & 
                           (tenor_df.trading_date <= dt.date.fromisoformat(end_date)) & 
                           (tenor_df.Year.isin(years))]
    products = filtered_df.Product.unique()
    n = len(products)
    color_grad = color_gradient((0, 87, 184), (151, 215, 0), n)
    colors = {products[i]: color_grad[i] for i in range(n)}
    
    futures_fig.update_layout(xaxis=dict(range=[filtered_df.trading_date.min(), filtered_df.trading_date.max()]))
    futures_fig.update_layout(yaxis=dict(range=[filtered_df.price.min()-10, filtered_df.price.max()+10]))
    for trace in futures_fig.data:
        if (trace.meta['tenor'] == tenor) & (trace.meta['peak'] == (peak == 'Peak')) & (trace.name in products):
            trace.visible = True
            trace.line.color = f'rgb{colors[trace.name]}'
        else: 
            trace.visible = False
    return futures_fig

@callback(
    Output('download-gas', 'data'),
    Input('download-button-gas', 'n_clicks'),
    Input('date-picker-range-gas', 'start_date'),
    Input('date-picker-range-gas', 'end_date'))
def download_spot(btn, start_date, end_date):
    if 'download-button-gas' == ctx.triggered_id:
        start, end = pd.Timestamp(start_date), pd.Timestamp(end_date) + pd.Timedelta(hours=23)
        filtered_df = gas_df_clean[start:end]
        return dcc.send_data_frame(filtered_df.to_excel, 
                                   f"{start_date.replace('-', '')}_{end_date.replace('-', '')}_gas_spot_prices.xlsx", 
                                   sheet_name=f'Gas Spot Prices')
    
@callback(
    Output('date-picker-range-gas', 'start_date'),
    Output('date-picker-range-gas', 'end_date'),
    Input('ytd-button-gas', 'n_clicks'),
    Input('qtd-button-gas', 'n_clicks'),
    Input('mtd-button-gas', 'n_clicks'),
    Input('all-button-gas', 'n_clicks'),
    Input('date-picker-range-gas', 'start_date'),
    Input('date-picker-range-gas', 'end_date'),
)
def update_gas_picker_range(ytd, qtd, mtd, all, start_date, end_date):
    last_timestamp = gas_df_clean.index[-1]
    if 'ytd-button-gas' == ctx.triggered_id:
        return dt.date(year=last_timestamp.year, month=1, day=1), last_timestamp.date()
    elif 'qtd-button-gas' == ctx.triggered_id:
        return dt.date(year=last_timestamp.year, month=(last_timestamp.quarter-1)*3 +1, day=1), last_timestamp.date()
    elif 'mtd-button-gas' == ctx.triggered_id:
        return dt.date(year=last_timestamp.year, month=last_timestamp.month, day=1), last_timestamp.date()
    elif 'all-button-gas' == ctx.triggered_id:
        return gas_df_clean.index[0].date(), last_timestamp.date()
    else:
        return start_date, end_date
    
@callback(
    Output('gas-graph', 'figure'),
    Input('date-picker-range-gas', 'start_date'),
    Input('date-picker-range-gas', 'end_date'))
def update_gas_figure(start_date, end_date):
    start, end = pd.Timestamp(start_date), pd.Timestamp(end_date) + pd.Timedelta(hours=23)
    filtered_df = gas_df_clean[start:end]
    gas_fig.update_layout(xaxis=dict(range=[filtered_df.index[0], filtered_df.index[-1]]))
    gas_fig.update_layout(yaxis=dict(range=[filtered_df['price'].min(), filtered_df['price'].max()+10]))
    return gas_fig

# Run the app inline in the notebook
app.run(jupyter_mode="tab", host='0.0.0.0')