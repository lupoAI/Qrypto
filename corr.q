// @fileOverview Enter a description here...
// load data and start analyzing it

\l C:/Users/salom/workspace/crypto/data/db

// @param sym1 {list data for ticker symbol 1} 
// @param sym2 {list data for ticker symbol 2} 
// @param lag {} 
// @returns {Type} Enter a return description here...
corrLag: {[sym1; sym2; lag] cov[lag _ sym1;(neg lag) _ sym2] % sqrt (var lag _ sym1) * var (neg lag) _ sym2}

autoCorrLag: {[sym; lag] corrLag[sym; sym; lag]}

corr: corrLag[;;0]

pctDelta: {0f, 100 * ((1 _ x) - (-1 _ x)) % (-1 _ x)}

getSymData: {select open_time, open from kline where sym=x}

symAutoCorr: {[symData; nLags] data: update priceDelta: pctDelta open from symData;
    autoCorrLag[exec priceDelta from data] each til nLags}

symSymJoin: {[sym1; sym2] symData1: getSymData sym1;
    symData2: getSymData sym2;
    data1: `open_time xkey update priceDelta1: pctDelta open, open1: open from symData1;
    data2: `open_time xkey update priceDelta2: pctDelta open, open2: open from symData2;
    delete open from data1 uj data2}

symSymCorr: {[symData1; symData2; nLags] data1: `open_time xkey update priceDelta1: pctDelta open, open1: open from symData1;
    data2: `open_time xkey update priceDelta2: pctDelta open, open2: open from symData2;
    dataJoined: data1 uj data2;
    dataJoined: dataJoined where all each not null dataJoined;
    corrLag[exec priceDelta1 from dataJoined; exec priceDelta2 from dataJoined] each til nLags}

nsMins: 60000000000;

groupByMinutes: {[minutes; symData] select first open by (minutes * nsMins) xbar open_time from symData}

groupByMinutesDelta: {[minutes; symData] update priceDelta: pctDelta open from select first open by (minutes * nsMins) xbar open_time from symData}

symAutoCorrAnalysis: {[sym; nLags] symData: getSymData sym;
    autoCorrLagsN: symAutoCorr[;nLags];
    groupSymByMinutes: groupByMinutes[; symData];
    analysis: ([] 
        tlag: til nLags;
        t1m: autoCorrLagsN symData;
        t5m: autoCorrLagsN groupSymByMinutes[5];
        t15m: autoCorrLagsN groupSymByMinutes[15];
        t30m: autoCorrLagsN groupSymByMinutes[30];
        t1h: autoCorrLagsN groupSymByMinutes[60];
        t3h: autoCorrLagsN groupSymByMinutes[3 * 60];
        t6h: autoCorrLagsN groupSymByMinutes[6 * 60];
        t12h: autoCorrLagsN groupSymByMinutes[12 * 60];
        t1d: autoCorrLagsN groupSymByMinutes[24 * 60]
        );
    (`$ (string[sym], "_") ,/:({1 _ x} each string cols analysis)) xcol analysis}


symSymCorrAnalysis: {[sym1; sym2; nLags] symData1: getSymData sym1;
    symData2: getSymData sym2;
    corrLagsN: symSymCorr[;;nLags];
    gSym1: groupByMinutes[; symData1];
    gSym2: groupByMinutes[; symData2];
    analysis: ([] 
        tlag: til nLags;
        t1m: corrLagsN[symData1;symData2];
        t5m: corrLagsN[gSym1[5];gSym2[5]];
        t15m: corrLagsN[gSym1[15];gSym2[15]];
        t30m: corrLagsN[gSym1[30];gSym2[30]];
        t1h: corrLagsN[gSym1[60];gSym2[60]];
        t3h: corrLagsN[gSym1[3 * 60];gSym2[3 * 60]];
        t6h: corrLagsN[gSym1[6 * 60];gSym2[6 * 60]];
        t12h: corrLagsN[gSym1[12 * 60];gSym2[12 * 60]];
        t1d: corrLagsN[gSym1[24 * 60];gSym2[24 * 60]]
        );
    (`$ (string[sym1], "_vs_", string[sym2], "_") ,/:({1 _ x} each string cols analysis)) xcol analysis}

loopCorr: {(enlist `corr) ! enlist corr[x[`priceDelta1]; x[`priceDelta2]]}

symSymRollCorr: {[sym1; sym2; minutes; lag] symData1: groupByMinutes[minutes] getSymData sym1;
    symData2: groupByMinutes[minutes] getSymData sym2;
    data1: delete open from `open_time xkey update priceDelta1: pctDelta open, open1: open from symData1;
    data2: delete open from `open_time xkey update priceDelta2: xprev[lag;pctDelta open], open2: xprev[lag;open] from symData2;
    dataJoined: data1 uj data2;
    dataGrouped: select priceDelta1, priceDelta2 by open_time.month from lag _ dataJoined;
    loopCorr each dataGrouped
    }




btcAutoCorr: symAutoCorr[`BTCUSDT; 30]


btcethCorr: symSymCorr[`BTCUSDT; `ETHUSDT; 30]

rollCorr: symSymRollCorr[`BTCUSDT; `ETHUSDT; 60; 24]




symSymCorrAnalysis[`ADAUSDT; `BTCUSDT; 35]
rollCorr: symSymRollCorr[`ADAUSDT; `BTCUSDT; 60; 24]


symSymCorrAnalysis[`XRPUSDT; `BTCUSDT; 35]
rollCorr: symSymRollCorr[`XRPUSDT; `BTCUSDT; 60; 24]




