const WebSocket = require('ws');
const moment = require('moment-timezone');
const createCsvWriter = require('csv-writer').createObjectCsvWriter;
require('colors');

const symbols = ['btcusdt', 'ethusdt', 'solusdt', 'bnbusdt', 'dogeusdt', 'wifusdt'];
const websocketUrlBase = 'wss://fstream.binance.com/ws';
const tradesFilename = 'binance_trades.csv';

const csvWriter = createCsvWriter({
    path: tradesFilename,
    header: [
        {id: 'eventTime', title: 'Event Time'},
        {id: 'symbol', title: 'Symbol'},
        {id: 'aggTradeId', title: 'Aggregate Trade Id'},
        {id: 'price', title: 'Price'},
        {id: 'quantity', title: 'Quantity'},
        {id: 'tradeTime', title: 'Trade Time'},
        {id: 'isBuyerMaker', title: 'Buyer Maker'},
    ],
    append: true,
});

const binanceTradeStream = async (uri, symbol) => {
    const ws = new WebSocket(uri);

    ws.on('open', () => {
        console.log(`Connected to ${uri}`);
    });

    ws.on('message', async (message) => {
        const data = JSON.parse(message);
        const eventTime = data.E;
        const aggTradeId = data.a;
        const price = parseFloat(data.p);
        const quantity = parseFloat(data.q);
        const tradeTime = data.T;
        const isBuyerMaker = data.m;

        const est = moment.tz(tradeTime, 'America/Sao_Paulo').format('HH:mm:ss');
        const usdSize = price * quantity;
        const displaySymbol = symbol.toUpperCase().replace('USDT', '');
        const tradeType = isBuyerMaker ? 'SELL' : 'BUY';
        const bgColor = isBuyerMaker ? 'bgRed' : 'bgGreen';

        if (usdSize > 14999) {
            const msg = `${tradeType} ${displaySymbol} ${est} $${usdSize.toFixed(2)}`;
            console.log(msg[bgColor].white);
            await csvWriter.writeRecords([
                {
                    eventTime,
                    symbol: displaySymbol,
                    aggTradeId,
                    price,
                    quantity,
                    tradeTime,
                    isBuyerMaker,
                },
            ]);
        }
    });

    ws.on('error', (err) => {
        console.log(`WebSocket error: ${err}`.red);
        setTimeout(() => binanceTradeStream(uri, symbol), 5000);
    });
};

const main = async () => {
    symbols.forEach((symbol) => {
        const streamUrl = `${websocketUrlBase}/${symbol}@aggTrade`;
        binanceTradeStream(streamUrl, symbol);
    });
};

main();
