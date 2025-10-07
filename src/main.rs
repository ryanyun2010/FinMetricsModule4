use std::collections::HashMap;
use indicatif::{ProgressBar, ProgressStyle};
use yahoo_finance_api::time::{macros::datetime, OffsetDateTime};
use yahoo_finance_api::{Decimal, YahooConnector};
use std::fs::File;
use std::io::Write;
use serde::{Serialize};

/* List of S&P 500 companies from 01/01/2008 to 12/31/2012 from https://gist.github.com/alexchinco/1ef9cb653334839f3c24#file-ticker-data-csv*/
const STOCKS_IN_SP_500: [&str; 542] = ["A", "AA", "AAPL", "ABBV", "ABC", "ABK", "ABT", "ACE", "ACN", "ACT", "ADBE", "ADI", "ADM", "ADP", "ADSK", "ADT", "AEE", "AEP", "AES", "AET", "AFL", "AGL", "AGN", "AIG", "AIV", "AIZ", "AKAM", "AKS", "ALL", "ALTR", "ALXN", "AMAT", "AMD", "AMGN", "AMP", "AMT", "AMZN", "AN", "ANF", "ANR", "AON", "APA", "APC", "APD", "APH", "APOL", "ARG", "ATI", "AVP", "AVY", "AXP", "AYE", "AZO", "BA", "BAC", "BAX", "BBBY", "BBT", "BBY", "BCR", "BDX", "BEAM", "BEN", "BF", "BHI", "BIG", "BIIB", "BJS", "BK", "BLK", "BLL", "BMC", "BMS", "BMY", "BRCM", "BRK", "BSX", "BTU", "BWA", "BXP", "C", "CA", "CAG", "CAH", "CAM", "CAT", "CB", "CBE", "CBG", "CBS", "CCI", "CCL", "CEG", "CELG", "CEPH", "CERN", "CF", "CFN", "CHK", "CHRW", "CI", "CINF", "CL", "CLF", "CLX", "CMA", "CMCSA", "CME", "CMG", "CMI", "CMS", "CNP", "CNX", "COF", "COG", "COH", "COL", "COP", "COST", "COV", "CPB", "CPWR", "CRM", "CSC", "CSCO", "CSX", "CTAS", "CTL", "CTSH", "CTXS", "CVC", "CVH", "CVS", "CVX", "D", "DD", "DE", "DELL", "DF", "DFS", "DG", "DGX", "DHI", "DHR", "DIS", "DISCA", "DLPH", "DLTR", "DNB", "DNR", "DO", "DOV", "DOW", "DPS", "DRI", "DTE", "DTV", "DUK", "DV", "DVA", "DVN", "EA", "EBAY", "ECL", "ED", "EFX", "EIX", "EK", "EL", "EMC", "EMN", "EMR", "EOG", "EP", "EQR", "EQT", "ESRX", "ESV", "ETFC", "ETN", "ETR", "EW", "EXC", "EXPD", "EXPE", "F", "FAST", "FCX", "FDO", "FDX", "FE", "FFIV", "FHN", "FII", "FIS", "FISV", "FITB", "FLIR", "FLR", "FLS", "FMC", "FOSL", "FRX", "FSLR", "FTI", "FTR", "GAS", "GCI", "GD", "GE", "GENZ", "GHC", "GILD", "GIS", "GLW", "GME", "GNW", "GPC", "GPS", "GR", "GRMN", "GS", "GT", "GWW", "HAL", "HAR", "HAS", "HBAN", "HCBK", "HCN", "HCP", "HD", "HES", "HIG", "HNZ", "HOG", "HON", "HOT", "HP", "HPQ", "HRB", "HRL", "HRS", "HSP", "HST", "HSY", "HUM", "IBM", "ICE", "IFF", "IGT", "INTC", "INTU", "IP", "IPG", "IR", "IRM", "ISRG", "ITT", "ITW", "IVZ", "JBL", "JCI", "JCP", "JDSU", "JEC", "JNJ", "JNPR", "JNS", "JOY", "JPM", "JWN", "K", "KEY", "KG", "KIM", "KLAC", "KMB", "KMI", "KMX", "KO", "KR", "KRFT", "KSS", "L", "LB", "LEG", "LEN", "LH", "LIFE", "LLL", "LLTC", "LLY", "LM", "LMT", "LNC", "LO", "LOW", "LRCX", "LSI", "LUK", "LUV", "LXK", "LYB", "M", "MA", "MAR", "MAS", "MAT", "MCD", "MCHP", "MCK", "MCO", "MDLZ", "MDT", "MEE", "MET", "MFE", "MHFI", "MHS", "MI", "MIL", "MJN", "MKC", "MMC", "MMI", "MMM", "MNST", "MO", "MOLX", "MON", "MOS", "MPC", "MRK", "MRO", "MS", "MSFT", "MSI", "MTB", "MU", "MUR", "MWV", "MWW", "MYL", "NBL", "NBR", "NDAQ", "NE", "NEE", "NEM", "NFLX", "NFX", "NI", "NKE", "NOC", "NOV", "NOVL", "NRG", "NSC", "NSM", "NTAP", "NTRS", "NU", "NUE", "NVDA", "NVLS", "NWL", "NWSA", "NYT", "NYX", "ODP", "OKE", "OMC", "ORCL", "ORLY", "OXY", "PAYX", "PBCT", "PBI", "PCAR", "PCG", "PCL", "PCLN", "PCP", "PCS", "PDCO", "PEG", "PEP", "PETM", "PFE", "PFG", "PG", "PGN", "PGR", "PH", "PHM", "PKI", "PLD", "PLL", "PM", "PNC", "PNR", "PNW", "POM", "PPG", "PPL", "PRGO", "PRU", "PSA", "PSX", "PWR", "PX", "PXD", "Q", "QCOM", "QEP", "R", "RAI", "RDC", "RF", "RHI", "RHT", "RL", "ROK", "ROP", "ROST", "RRC", "RRD", "RSG", "RSH", "RTN", "S", "SAI", "SBUX", "SCG", "SCHW", "SE", "SEE", "SGP", "SHLD", "SHW", "SIAL", "SII", "SJM", "SLB", "SLE", "SLM", "SNA", "SNDK", "SNI", "SO", "SPG", "SPLS", "SRCL", "SRE", "STI", "STJ", "STR", "STT", "STX", "STZ", "SUN", "SVU", "SWK", "SWN", "SWY", "SYK", "SYMC", "SYY", "T", "TAP", "TDC", "TE", "TEG", "TEL", "TER", "TGT", "THC", "TIE", "TIF", "TJX", "TLAB", "TMK", "TMO", "TRIP", "TROW", "TRV", "TSN", "TSO", "TSS", "TWC", "TXN", "TXT", "TYC", "UNH", "UNM", "UNP", "UPS", "URBN", "USB", "UTX", "V", "VAR", "VFC", "VIAB", "VLO", "VMC", "VNO", "VRSN", "VTR", "VZ", "WAG", "WAT", "WDC", "WEC", "WFC", "WFM", "WFR", "WHR", "WIN", "WLP", "WM", "WMB", "WMT", "WPX", "WU", "WY", "WYN", "WYNN", "X", "XEL", "XL", "XLNX", "XOM", "XRAY", "XRX", "XTO", "XYL", "YHOO", "YUM", "ZION", "ZMH"];

#[derive(Debug)]
struct DataPoint {
    year: u16,
    month: u16,
    day: u16,
    dividend: Decimal,
    close: Decimal,
    tbill: Decimal
}


#[derive(Debug)]
struct PriceDataPoint {
    year: u16,
    month: u16,
    day: u16,
    close: Decimal,
}

#[derive(Debug)]
struct DataPointTBill {
    time: OffsetDateTime,
    tbill: Decimal
}


#[derive(Debug, Serialize)]
struct Estimate {
    year: u16,
    month: u16,
    day: u16,
    guess: f64,
    real: f64,
    percent_error: f64
}




fn main() -> Result<(), Box<dyn std::error::Error>>{
    let mut provider = yahoo_finance_api::YahooConnector::new().unwrap();
    let irx = get_irx_data(&mut provider, datetime!(1990-1-1 00:00:00.00 UTC), datetime!(2020-12-31 23:59:59.99 UTC))?;
    let mut data = HashMap::new();
    let mut just_price_data = HashMap::new();
    println!("Fetching stock data");
    let prog = ProgressBar::new(STOCKS_IN_SP_500.len() as u64);
    prog.set_style(ProgressStyle::with_template("[{elapsed_precise}] {bar:40.white/gray} {pos:>7}/{len:7} {msg}").unwrap());
    let mut failures = vec![];
    for ticker in STOCKS_IN_SP_500.iter() {
        if let Ok((x,y)) = get_data(&mut provider, &irx,ticker, datetime!(1990-1-1 0:00:00.00 UTC), datetime!(2020-12-31 23:59:59.99 UTC)){
            if x.len() > 0 {
                data.insert(ticker, x);
                just_price_data.insert(ticker,y);
            }else {
                failures.push(ticker);
            }
        }else {
            failures.push(ticker);
        }
        
        
        prog.inc(1);
    }
    prog.finish();
    println!("Failed to fetch data for {} tickers: {:?}", failures.len(), failures);
    println!("Step 1. modeling prices based on discounted future dividends");
    let prog = ProgressBar::new(data.len() as u64);
    prog.set_style(ProgressStyle::with_template("[{elapsed_precise}] {bar:40.white/gray} {pos:>7}/{len:7} {msg}").unwrap());
    let mut step_1_prices = HashMap::new();
    /* I will use 1990-2010 estimate prices for using the model, 2010-2020 will always be the future*/
    for (stock, dat) in data.iter() {
        let mut sp1p = Vec::new();
        let mut i = 0;
        while dat[i].year <= 2010 {
            let tbill = dat[i].tbill;
            let mut value = 0.;
            let discount_factor = 1.0/(1.0+tbill/100.0);
            for (j, d) in dat.iter().enumerate().skip(i+1){
                value += discount_factor.powf((d.year * 12 + d.month - dat[i].year * 12 - dat[i].month) as f64/12.0) * d.dividend;
            }
            sp1p.push(Estimate {
                year: dat[i].year,
                month: dat[i].month,
                day: dat[i].day,
                guess: value,
                real: dat[i].close,
                percent_error: (value - dat[i].close)/dat[i].close * 100.


            });
            i += 1;
            if i >= dat.len() {
                break;
            }
        }
        step_1_prices.insert(**stock,sp1p);
        prog.inc(1);
    }
    prog.finish();


    println!("Step 2. optimizing discount rate to best estimate prices");
    let mut step_2_implied_dividend_rate = HashMap::new();
    let prog = ProgressBar::new(data.len() as u64);
    prog.set_style(ProgressStyle::with_template("[{elapsed_precise}] {bar:40.white/gray} {pos:>7}/{len:7} {msg}").unwrap());
    for (stock, dat) in data.iter() {
        let mut sp2r = Vec::new();
        let mut i = 0;
        while dat[i].year <= 2010 {
            let mut best = None;
            let real_price = dat[i].close;

            let mut discount_estimate = 0.1;
            while discount_estimate < 15.0 {
                let mut value = 0.;
                let discount_factor: f64 = 1.0/(1.0+discount_estimate/100.0);
                for (j, d) in dat.iter().enumerate().skip(i+1){
                    value += discount_factor.powf((d.year * 12 + d.month - dat[i].year * 12 - dat[i].month) as f64/12.0) * d.dividend;
                }

                let delta = (value - real_price).abs();
                if best.unwrap_or((0.0,f64::MAX)).1 > delta {
                    best = Some((discount_estimate, delta));
                }
                discount_estimate += 0.1;
            }
            sp2r.push(Estimate {
                year: dat[i].year,
                month: dat[i].month,
                day: dat[i].day,
                guess:best.unwrap().0,

                real: dat[i].tbill,
                percent_error: (best.unwrap().0 - dat[i].tbill)/dat[i].tbill * 100.

            });
            i += 1;
            if i >= dat.len() {
                break;
            }
        }
        step_2_implied_dividend_rate.insert(**stock,sp2r);
        prog.inc(1);
    }
    prog.finish();


    println!("Step 3. modeling prices based on discounted future dividends without cheating by knowing future dividends");
    let prog = ProgressBar::new(data.len() as u64);
    prog.set_style(ProgressStyle::with_template("[{elapsed_precise}] {bar:40.white/gray} {pos:>7}/{len:7} {msg}").unwrap());
    let mut step_3_prices = HashMap::new();
    /* I will use 1990-2010 estimate prices for using the model, 2010-2020 will always be the future*/
    for (stock, dat) in data.iter() {
        let mut sp3p = Vec::new();
        let mut i = 0;
        while dat[i].year <= 2010 {
            let tbill = dat[i].tbill;
            let mut value = 0.;
            let discount_factor = 1.0/(1.0+tbill/100.0);
            let latest_dividend = dat[i].dividend;
            for i in 0..(2020 * 12 + 12 - dat[i].year * 12 - dat[i].month){
                value += discount_factor.powi(i.into()) * latest_dividend;
            }
            sp3p.push(Estimate {
                year: dat[i].year,
                month: dat[i].month,
                day: dat[i].day,
                guess: value,
                real: dat[i].close,
                percent_error: (value - dat[i].close)/dat[i].close * 100.
            });
            i += 1;
            if i >= dat.len() {
                break;
            }
        }
        step_3_prices.insert(**stock,sp3p);
        prog.inc(1);
    }
    prog.finish();


    println!("Step 4. optimizing discount rate to best estimate prices without cheating by knowing future dividends");
    let mut step_4_implied_dividend_rate = HashMap::new();
    let prog = ProgressBar::new(data.len() as u64);
    prog.set_style(ProgressStyle::with_template("[{elapsed_precise}] {bar:40.white/gray} {pos:>7}/{len:7} {msg}").unwrap());
    for (stock, dat) in data.iter() {
        let mut sp4r = Vec::new();
        let mut i = 0;
        while dat[i].year <= 2010 {
            let mut best = None;
            let real_price = dat[i].close;

            let latest_dividend = dat[i].dividend;
            let mut discount_estimate = 0.1;
            while discount_estimate < 15.0 {

                let mut value = 0.;
                let discount_factor: f64 = 1.0/(1.0+discount_estimate/100.0);
                for i in 0..(2020 * 12 + 12 - dat[i].year * 12 - dat[i].month){
                    value += discount_factor.powi(i.into()) * latest_dividend;
                }

                let delta = (value - real_price).abs();
                if best.unwrap_or((0.0,f64::MAX)).1 >= delta {
                    best = Some((discount_estimate, delta));
                }
                discount_estimate += 0.1;
            }
            sp4r.push(Estimate {
                year: dat[i].year,
                month: dat[i].month,
                day: dat[i].day,
                guess:best.unwrap().0,
                real: dat[i].tbill,
                percent_error: (best.unwrap().0 - dat[i].tbill)/dat[i].tbill * 100.
            });
            i += 1;
            if i >= dat.len() {
                break;
            }
        }
        step_4_implied_dividend_rate.insert(**stock,sp4r);
        prog.inc(1);
    }
    prog.finish();



    println!("Step 5. creating optimal portfolios based on step 1 prices");


    for year in 1990..=2010 {
        let mut step1_prices_collapsed: Vec<(String, f64, f64)> = vec![];
        let mut portfolio = HashMap::new();

        for s in step_1_prices.iter() {
            let most_recent_cur_year_estimate = s.1.iter().filter(|x| x.year == year).next_back();
            if let Some(x) = most_recent_cur_year_estimate {
                step1_prices_collapsed.push((s.0.to_string(), x.percent_error, x.real));
            }
        }


        let undervalued_stocks: Vec<_> = step1_prices_collapsed.into_iter()
            .filter(|&(_, percent_diff, _)| percent_diff > 0.0)
            .collect();

        let total: f64 = undervalued_stocks.iter().map(|&(_, p, _)| p).sum();

        if total > 0.0 {
            for (name, percent_diff, price) in undervalued_stocks.iter() {
                let weight = (percent_diff / total) * 100.0;
                portfolio.insert(name.to_string(),( weight, price));
            }
        }


        let mut real_prices_in_10_years: HashMap<String, f64> = HashMap::new();
        for (name, _,_) in undervalued_stocks.iter() {
            real_prices_in_10_years.insert(name.clone(),just_price_data.get(&name.as_str()).unwrap().iter().filter(|x| x.year == year + 10).next_back().map(|x| x.close).unwrap_or(just_price_data.get(&name.as_str()).unwrap().iter().filter(|x|( x.year as i32 -(year + 10) as i32).abs() <= 1).next_back().map(|x| x.close).unwrap()));
        }

        let mut total_gain = 0.;
        for stock in  portfolio{
            total_gain += real_prices_in_10_years.get(&stock.0).unwrap()/stock.1.1 * stock.1.0/100.; 
        } 
        println!("10 year gain using model (with step 1 prices) (with step 1 prices) (with step 1 prices) (with step 1 prices) (with step 1 prices) (with step 1 prices) (with step 1 prices) (with step 1 prices) (with step 1 prices) in {:?}: {:?}, {:?}", year, total_gain, total_gain.powf(1./10.));
    }   

    println!("Step 6. creating optimal portfolios based on step 3 prices");


    for year in 1990..=2010 {
        let mut step3_prices_collapsed: Vec<(String, f64, f64)> = vec![];
        let mut portfolio = HashMap::new();

        for s in step_3_prices.iter() {
            let most_recent_cur_year_estimate = s.1.iter().filter(|x| x.year == year).next_back();
            if let Some(x) = most_recent_cur_year_estimate {
                step3_prices_collapsed.push((s.0.to_string(), x.percent_error, x.real));
            }
        }


        let undervalued_stocks: Vec<_> = step3_prices_collapsed.into_iter()
            .filter(|&(_, percent_diff, _)| percent_diff > 0.0)
            .collect();

        let total: f64 = undervalued_stocks.iter().map(|&(_, p, _)| p).sum();

        if total > 0.0 {
            for (name, percent_diff, price) in undervalued_stocks.iter() {
                let weight = (percent_diff / total) * 100.0;
                portfolio.insert(name.to_string(),( weight, price));
            }
        }
        println!("TOTAL WEIGHT: {:?}", portfolio.iter().map(|x| x.1.0).sum::<f64>());


        let mut real_prices_in_10_years: HashMap<String, f64> = HashMap::new();
        for (name, _,_) in undervalued_stocks.iter() {
            real_prices_in_10_years.insert(name.clone(),just_price_data.get(&name.as_str()).unwrap().iter().filter(|x| x.year == year + 10).next_back().map(|x| x.close).unwrap_or(just_price_data.get(&name.as_str()).unwrap().iter().filter(|x|( x.year as i32 -(year + 10) as i32).abs() <= 1).next_back().map(|x| x.close).unwrap_or(0.)));
        }

        let mut total_gain = 0.;
        for stock in  portfolio{
            total_gain += real_prices_in_10_years.get(&stock.0).unwrap()/stock.1.1 * stock.1.0/100.; 
        } 
        println!("10 year gain using model (with step 3 prices) in {:?}: {:?}, {:?}", year, total_gain, total_gain.powf(1./10.));
    }   



    let prog = ProgressBar::new(data.len() as u64);
    prog.set_style(ProgressStyle::with_template("[{elapsed_precise}] {bar:40.white/gray} {pos:>7}/{len:7} {msg}").unwrap());



    println!("Serializing output");

    let prog = ProgressBar::new(4);
    prog.set_style(ProgressStyle::with_template("[{elapsed_precise}] {bar:40.white/gray} {pos:>7}/{len:7} {msg}").unwrap());
    let step_1_serialized = serde_json::to_string_pretty(&step_1_prices).expect("Serialization failed");
    prog.inc(1);
    let step_2_serialized = serde_json::to_string_pretty(&step_2_implied_dividend_rate).expect("Serialization failed");
    prog.inc(1);
    let step_3_serialized = serde_json::to_string_pretty(&step_3_prices).expect("Serialization failed");
    prog.inc(1);
    let step_4_serialized = serde_json::to_string_pretty(&step_4_implied_dividend_rate).expect("Serialization failed");
    prog.finish();

    println!("Writing output to disk");
    let prog = ProgressBar::new(4);
    prog.set_style(ProgressStyle::with_template("[{elapsed_precise}] {bar:40.white/gray} {pos:>7}/{len:7} {msg}").unwrap());
    let mut file1 = File::create("step1.json")?;
    file1.write_all(step_1_serialized.as_bytes())?;
    prog.inc(1);
    let mut file2 = File::create("step2.json")?;
    file2.write_all(step_2_serialized.as_bytes())?;
    prog.inc(1);
    let mut file3 = File::create("step3.json")?;
    file3.write_all(step_3_serialized.as_bytes())?;
    prog.inc(1);
    let mut file4 = File::create("step4.json")?;
    file4.write_all(step_4_serialized.as_bytes())?;
    prog.finish();



    Ok(())


}


fn get_data(provider: &mut YahooConnector, irx: &[DataPointTBill], ticker: &str, start: OffsetDateTime, end: OffsetDateTime) -> Result<(Vec<DataPoint>, Vec<PriceDataPoint>),Box<dyn std::error::Error>> {
    let resp = tokio_test::block_on(provider.get_quote_history_interval(ticker, start, end, "1d"))?;
    let div = resp.dividends()?;
    let quotes = resp.quotes()?;

    let mut data: Vec<DataPoint> = Vec::new();
    let mut div_offset = 0;
    let mut quote_offset = 0;
    let mut cur_irx = 0;
    let mut i = 0;
    let mut price_data = Vec::new();

    while i < usize::max(div.len(), quotes.len()){
        if let Some(d) = div.get(i + div_offset)&& let Some(q) = quotes.get(i + quote_offset){
            if d.date - 300_000 > q.timestamp {
                quote_offset += 1;
                continue;
            }
            else if q.timestamp - 300_000 > d.date {
                div_offset += 1;
                continue;
            }else {

                let time = OffsetDateTime::from_unix_timestamp(d.date)?;
                while cur_irx < irx.len() - 1 && (irx[cur_irx].time.unix_timestamp() - d.date).abs() >= 300_000 {
                    cur_irx += 1;
                }
                if (irx[cur_irx].time.unix_timestamp() - d.date).abs() < 300_000 {
                    data.push(
                        DataPoint {
                            year: time.year() as u16, 
                            month: time.month() as u16,
                            day: time.day() as u16,
                            dividend: d.amount,
                            close: q.close,
                            tbill: irx[cur_irx].tbill
                        }
                    )
                }else {
                    panic!("Couldn't find tbill rate for {:?} {:?} {:?}", time.month(), time.day(), time.year());
                }
            }

        }
        i += 1;
            
    }

    for quote in quotes.iter() {
        let time = OffsetDateTime::from_unix_timestamp(quote.timestamp)?;
        price_data.push(PriceDataPoint {
            year: time.year() as u16,
            month: time.month() as u16,
            day: time.day() as u16,
            close: quote.close});
    }

    assert_eq!(data.len(), div.len(), "the amount of data points returned should be equal to the number of dividends given out during the time period, otherwise we have most likely failed to find pricing information for that date");
    Ok((data, price_data))
}



fn get_irx_data(provider: &mut YahooConnector, start: OffsetDateTime, end: OffsetDateTime) -> Result<Vec<DataPointTBill>,Box<dyn std::error::Error>> {
    let resp = tokio_test::block_on(provider.get_quote_history_interval("^IRX", start, end, "1d"))?;
    let quotes = resp.quotes()?;

    Ok(quotes.iter().map(|x| DataPointTBill {
            time: OffsetDateTime::from_unix_timestamp(x.timestamp).unwrap(),
            tbill: x.close}
        ).collect())
}
