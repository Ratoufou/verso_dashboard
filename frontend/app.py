from collect_transform_data import *
from figures import *
from utils import color_gradient
from dash import Dash, html, dcc, callback, Input, Output, ctx
import datetime as dt
import dash_bootstrap_components as dbc


# Get data
spot_df = get_spot_prices()
futures_df = get_futures_prices()
gas_df = filter_gas_df(get_gas_prices())


# Preliminary calculations
tenor_info = gather_tenor_info(futures_df=futures_df, 
                               spot_df=spot_df)
products_evolution_tab = build_products_evolution_tab(futures_df=futures_df)
spot_df_dict = {'Hourly' : spot_df.set_index('delivery_start')[['price']].sort_index()}
spot_df_dict['Daily'] = spot_df_dict['Hourly'].resample('D').mean().round(1)
spot_df_dict['Monthly'] = spot_df_dict['Hourly'].resample('MS').mean().round(1)


# Figures 
spot_fig = plot_spot_fig(spot_df_dict=spot_df_dict,
                         tenor_info=tenor_info)
futures_fig = plot_futures_fig(futures_df)
gas_fig = plot_gas_fig(gas_df)
tab_fig = products_evolution_fig(products_evolution_tab)


# Other
today = dt.date.today()
indices_to_marks = {i: int(e) for i, e in enumerate(sorted(list(futures_df.delivery_year.unique())))}
marks_to_indices = {v: k for k, v in indices_to_marks.items()}
default_futures_df = futures_df[(futures_df.tenor == 'Year') & (futures_df.delivery_year.isin([today.year + i for i in range(1,4)]))]
futures_end = default_futures_df.trading_date.max()
futures_start = default_futures_df.trading_date.min()


# App
app = Dash(__name__,
           external_scripts=[{'src': 'https://cdnjs.cloudflare.com/ajax/libs/jszip/3.10.1/jszip.min.js'},
                             {'src': 'https://cdnjs.cloudflare.com/ajax/libs/html2pdf.js/0.9.2/html2pdf.bundle.js'}])

app.layout = html.Div(id = 'dash_page', children = [
    html.Div(className = 'horizontal_container', children = [
        html.H1(
            className = 'heading page_title',
            children = f'Energy Markets Dashboard - {today.strftime(format="%d/%m/%Y")}'),
        html.Img(
            className = 'logo',
            src = "assets/logo_bleu.png")
    ]),
    html.H2(
        className = 'heading section_title',
        children='Power'),
    html.Div(className = 'horizontal_container', children = [
        html.Div(className = 'figure_container', children = [
            html.Div(className = 'horizontal_container menu', children = [
                dcc.Input(
                    id = 'start-date-spot',
                    className = 'menu_item',
                    type = 'date',
                    value = today - dt.timedelta(days=today.day-1)),
                dcc.Input(
                    id = 'end-date-spot',
                    className = 'menu_item',
                    type = 'date',
                    value = today + dt.timedelta(days=1)),
                html.Button(
                    'YTD',
                    id = 'ytd-button-spot',
                    className = 'menu_item'),
                html.Button(
                    'QTD',
                    id = 'qtd-button-spot',
                    className = 'menu_item'),
                html.Button(
                        'MTD',
                    id = 'mtd-button-spot',
                    className = 'menu_item'),
                html.Button(
                    'All',
                    id = 'all-button-spot',
                    className = 'menu_item'),
                dbc.Select(
                    id = 'sampling-dropdown-spot',
                    className = 'menu_item',
                    options = [{'label': sampling, 'value': sampling} for sampling in ['Hourly', 'Daily', 'Monthly']],
                    value = 'Hourly'),
                dbc.Select(
                    id = 'tenor-dropdown-spot',
                    className = 'menu_item',
                    options = [{'label': tenor, 'value': tenor} for tenor in ['None', 'Month', 'Quarter', 'Year']],
                    value = 'None'),
                html.Button(
                    'Download',
                    id = 'download-button-spot',
                    className = 'menu_item'),
                dcc.Download(
                    id = 'download-spot')
            ]),
            dcc.Graph(
                id = 'graph-spot',
                figure = spot_fig)
        ]),
        html.Div(className = 'figure_container', children = [
            html.Div(className = 'horizontal_container menu', children = [
                dcc.Input(
                    id = 'start-date-futures',
                    className = 'menu_item',
                    type = 'date',
                    value = futures_start),
                dcc.Input(
                    id = 'end-date-futures',
                    className = 'menu_item',
                    type = 'date',
                    value = futures_end),
                html.Button(
                    'YTD',
                    id = 'ytd-button-futures',
                    className = 'menu_item'),
                html.Button(
                    'QTD',
                    id = 'qtd-button-futures',
                    className = 'menu_item'),
                html.Button(
                    'MTD',
                    id = 'mtd-button-futures',
                    className = 'menu_item'),
                html.Button(
                    'All',
                    id = 'all-button-futures',
                    className = 'menu_item'),
                dbc.Select(
                    id = 'tenor-dropdown-futures',
                    className = 'menu_item',
                    options = [{'label': sampling, 'value': sampling} for sampling in ['Year', 'Quarter', 'Month']],
                    value = 'Year'),
                dbc.Select(
                    id = 'peak-dropdown-futures',
                    className = 'menu_item',
                    options = [{'label': peak, 'value': peak} for peak in ['Base', 'Peak']],
                    value = 'Base'),
                dbc.Button(
                    'Download',
                    id = 'download-button-futures',
                    className = 'menu_item'),
                dcc.Download(
                    id = 'download-futures')
            ]),
            dcc.Graph(
                id = 'graph-futures',
                figure = futures_fig), 
            dcc.RangeSlider(
                id='yearslider',
                min=0, 
                max=len(indices_to_marks)-1, 
                marks={k: (v if v%5 == 0 else '') for k, v in indices_to_marks.items()},
                value=[marks_to_indices[today.year]+1, marks_to_indices[today.year]+3], 
                updatemode='mouseup',
                step=None,
                allowCross=False),            
        ]),
    ]),
    html.H3('Futures Prices and Trends**'),
    html.Div(className = 'tab_container', children = [
        dcc.Graph(
            id = 'graph-tab',
            figure = tab_fig)
    ]),
    html.Footer(
        '* For futures prices, the value used is the average of all prices at which a trade has taken place (without weighting).',
        className = 'footer'
    ),
    html.Footer(
         '** For prices and trends, average quotations for the current and previous days are taken into account.',
        className = 'footer'
    ),
    html.H2(
        className = 'heading section_title',
        children = 'Gas'),
    html.Div(className = 'horizontal_container', children = [
        html.Div(className = 'figure_container', children = [
            html.Div(className = 'horizontal_container menu', children = [
                dcc.Input(
                    id = 'start-date-gas',
                    className = 'menu_item',
                    type = 'date',
                    value = today - dt.timedelta(days=today.day-1)),
                dcc.Input(
                    id = 'end-date-gas',
                    className = 'menu_item',
                    type = 'date',
                    value = today),
                html.Button(
                    'YTD',
                    id = 'ytd-button-gas',
                    className = 'menu_item'),
                html.Button(
                    'QTD',
                    id = 'qtd-button-gas',
                    className = 'menu_item'),
                html.Button(
                    'MTD',
                    id = 'mtd-button-gas',
                    className = 'menu_item'),
                html.Button(
                    'All',
                    id = 'all-button-gas',
                    className = 'menu_item'),
                html.Button(
                    'Download',
                    id = 'download-button-gas',
                    className = 'menu_item'),
                dcc.Download(
                    id = 'download-gas')
            ]),
            dcc.Graph(
                id = 'graph-gas',
                figure = gas_fig)
        ]) 
    ]),
    html.Button(
        'PDF',
        id = 'pdf-button',
        className = 'menu_item',
        n_clicks = 0),
])

@callback(
    Output('download-spot', 'data'),
    Input('download-button-spot', 'n_clicks'),
    Input('start-date-spot', 'value'),
    Input('end-date-spot', 'value'),
    Input('sampling-dropdown-spot', 'value'))
def download_spot(btn, start_date, end_date, sampling):
    if 'download-button-spot' == ctx.triggered_id:
        start, end = pd.Timestamp(start_date), pd.Timestamp(end_date) + pd.Timedelta(hours=23)
        filtered_df = spot_df_dict[sampling][start:end]
        return dcc.send_data_frame(filtered_df.to_excel, 
                                   f"{start_date.replace('-', '')}_{end_date.replace('-', '')}_spot_prices.xlsx", 
                                   sheet_name=f'{sampling} Spot Prices')

@callback(
    Output('start-date-spot', 'value'),
    Output('end-date-spot', 'value'),
    Input('ytd-button-spot', 'n_clicks'),
    Input('qtd-button-spot', 'n_clicks'),
    Input('mtd-button-spot', 'n_clicks'),
    Input('all-button-spot', 'n_clicks'),
    Input('start-date-spot', 'value'),
    Input('end-date-spot', 'value'))
def update_spot_picker_range(ytd, qtd, mtd, all, start_date, end_date):
    last_timestamp = spot_df_dict['Hourly'].index[-1]
    if 'ytd-button-spot' == ctx.triggered_id:
        return dt.date(year=last_timestamp.year, month=1, day=1), last_timestamp.date()
    elif 'qtd-button-spot' == ctx.triggered_id:
        return dt.date(year=last_timestamp.year, month=(last_timestamp.quarter-1)*3 +1, day=1), last_timestamp.date()
    elif 'mtd-button-spot' == ctx.triggered_id:
        return dt.date(year=last_timestamp.year, month=last_timestamp.month, day=1), last_timestamp.date()
    elif 'all-button-spot' == ctx.triggered_id:
        return spot_df_dict['Hourly'].index[0].date(), last_timestamp.date()
    else:
        return start_date, end_date
   
@callback(
    Output('graph-spot', 'figure'),
    Input('start-date-spot', 'value'),
    Input('end-date-spot', 'value'),
    Input('sampling-dropdown-spot', 'value'),
    Input('tenor-dropdown-spot', 'value'))
def update_spot_figure(start_date, end_date, sampling, tenor):
    try:
        start, end = pd.Timestamp(start_date), pd.Timestamp(end_date) + pd.Timedelta(hours=23)
        filtered_df = spot_df_dict[sampling][start:end]
        spot_fig.update_layout(xaxis=dict(range=[filtered_df.index[0], filtered_df.index[-1]]))
        spot_fig.update_layout(yaxis=dict(range=[filtered_df['price'].min()-10, filtered_df['price'].max()+10]))
        if tenor == 'None':
            subtitle = ''
        else:
            subtitle = f'(with {tenor} Futures details*)'
        spot_fig.update_layout(title=f'{sampling} FR Spot Prices {subtitle}')
        for trace in spot_fig.data:
            if (trace.name == sampling) or (trace.legendgroup == tenor):
                trace.visible = True
            else:
                trace.visible = False
    except IndexError: 
        pass
    return spot_fig

@callback(
    Output('download-futures', 'data'),
    Input('download-button-futures', 'n_clicks'),
    Input('tenor-dropdown-futures', 'value'),
    Input('peak-dropdown-futures', 'value'),
    Input('yearslider', 'value'),
    Input('start-date-futures', 'value'),
    Input('end-date-futures', 'value'))
def download_futures(btn, tenor, peak, value, start_date, end_date):
    if 'download-button-futures' == ctx.triggered_id:
        years = sorted([indices_to_marks[i] for i in range(value[0], value[1]+1)])
        filtered_df = futures_df[(futures_df.tenor == tenor) &
                                 (futures_df.peak == peak) & 
                                 (futures_df.delivery_year.isin(years)) &
                                 (futures_df.trading_date >= dt.date.fromisoformat(start_date)) &
                                 (futures_df.trading_date <= dt.date.fromisoformat(end_date))][['trading_date', 
                                                                                                'tenor', 
                                                                                                'delivery_year',
                                                                                                'product_index', 
                                                                                                'type', 
                                                                                                'settlement_price']]
        return dcc.send_data_frame(filtered_df.to_excel, 
                                   f"{start_date.replace('-', '')}_{end_date.replace('-', '')}_futures_prices.xlsx", 
                                   sheet_name=f'{tenor} Futures Prices',
                                   index=False)
        
@callback(
    Output('start-date-futures', 'value'),
    Output('end-date-futures', 'value'),
    Input('ytd-button-futures', 'n_clicks'),
    Input('qtd-button-futures', 'n_clicks'),
    Input('mtd-button-futures', 'n_clicks'),
    Input('all-button-futures', 'n_clicks'),
    Input('start-date-futures', 'value'),
    Input('end-date-futures', 'value'))
def update_futures_picker_range(ytd, qtd, mtd, all, start_date, end_date):
    if 'ytd-button-futures' == ctx.triggered_id:
        return dt.date(year=today.year, month=1, day=1), today
    elif 'qtd-button-futures' == ctx.triggered_id:
        return dt.date(year=today.year, month=((today.month-1)//3)*3 +1, day=1), today
    elif 'mtd-button-futures' == ctx.triggered_id:
        return dt.date(year=today.year, month=today.month, day=1), today
    elif 'all-button-futures' == ctx.triggered_id:
        return futures_df.trading_date.min(), today
    else:
        return start_date, end_date
    
@callback(
    Output('graph-futures', 'figure'),
    Input('tenor-dropdown-futures', 'value'),
    Input('peak-dropdown-futures', 'value'),
    Input('start-date-futures', 'value'),
    Input('end-date-futures', 'value'),
    Input('yearslider', 'value'))
def update_futures_fig(tenor, peak, start_date, end_date, value):
    try:
        years = sorted([indices_to_marks[i] for i in range(value[0], value[1]+1)])
        filtered_df = futures_df[(futures_df.tenor == tenor) & 
                                 (futures_df.type == peak) & 
                                 (futures_df.trading_date >= dt.date.fromisoformat(start_date)) & 
                                 (futures_df.trading_date <= dt.date.fromisoformat(end_date)) & 
                                 (futures_df.delivery_year.isin(years))]
        products = sorted(list(set(zip(filtered_df.delivery_year, filtered_df.product_index))))
        n = len(products)
        color_grad = color_gradient((0, 87, 184), (151, 215, 0), n)
        colors = {products[i]: color_grad[i] for i in range(n)}
        
        futures_fig.update_layout(xaxis=dict(range=[filtered_df.trading_date.min(), filtered_df.trading_date.max()]))
        futures_fig.update_layout(yaxis=dict(range=[filtered_df.settlement_price.min()-10, filtered_df.settlement_price.max()+10]))
        futures_fig.update_layout(title=f'{peak} {tenor} Futures Prices Trend')
        for trace in futures_fig.data:
            if (trace.meta['tenor'] == tenor) & (trace.meta['type'] == peak) & ((trace.meta['year'], trace.meta['product_index']) in products):
                trace.visible = True
                trace.line.color = f"rgb{colors[(trace.meta['year'], trace.meta['product_index'])]}"
            else: 
                trace.visible = False
    except IndexError:
        pass
    return futures_fig

@callback(
    Output('download-gas', 'data'),
    Input('download-button-gas', 'n_clicks'),
    Input('start-date-gas', 'value'),
    Input('end-date-gas', 'value'))
def download_gas(btn, start_date, end_date):
    if 'download-button-gas' == ctx.triggered_id:
        start, end = dt.date.fromisoformat(start_date), dt.date.fromisoformat(end_date)
        filtered_df = gas_df[start:end]
        return dcc.send_data_frame(filtered_df.to_excel, 
                                   f"{start_date.replace('-', '')}_{end_date.replace('-', '')}_gas_spot_prices.xlsx", 
                                   sheet_name=f'Gas Spot Prices')
    
@callback(
    Output('start-date-gas', 'value'),
    Output('end-date-gas', 'value'),
    Input('ytd-button-gas', 'n_clicks'),
    Input('qtd-button-gas', 'n_clicks'),
    Input('mtd-button-gas', 'n_clicks'),
    Input('all-button-gas', 'n_clicks'),
    Input('start-date-gas', 'value'),
    Input('end-date-gas', 'value'))
def update_gas_picker_range(ytd, qtd, mtd, all, start_date, end_date):
    last_date = gas_df.index[-1]
    if 'ytd-button-gas' == ctx.triggered_id:
        return dt.date(year=last_date.year, month=1, day=1), last_date
    elif 'qtd-button-gas' == ctx.triggered_id:
        return dt.date(year=last_date.year, month=((last_date.month-1)//3)*3 +1, day=1), last_date
    elif 'mtd-button-gas' == ctx.triggered_id:
        return dt.date(year=last_date.year, month=last_date.month, day=1), last_date
    elif 'all-button-gas' == ctx.triggered_id:
        return gas_df.index[0], last_date
    else:
        return start_date, end_date
    
@callback(
    Output('graph-gas', 'figure'),
    Input('start-date-gas', 'value'),
    Input('end-date-gas', 'value'))
def update_gas_figure(start_date, end_date):
    try:
        start, end = dt.date.fromisoformat(start_date), dt.date.fromisoformat(end_date)
        filtered_df = gas_df[start:end]
        gas_fig.update_layout(xaxis=dict(range=[filtered_df.index[0], filtered_df.index[-1]]))
        gas_fig.update_layout(yaxis=dict(range=[filtered_df['last_price'].min(), filtered_df['last_price'].max()+10]))
    except IndexError:
        pass
    return gas_fig    

app.run(host='0.0.0.0')