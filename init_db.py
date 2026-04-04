import sqlite3
import os

DB_NAME = "trading_system.db"

def init_db():
    # 若存在則先砍掉重建供測試方便 (如果未來不想清空，可把這兩行拿掉)
    if os.path.exists(DB_NAME):
        os.remove(DB_NAME)

    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()

    # 1. 資金池資料表
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS Portfolios (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT UNIQUE NOT NULL,
        t0_cash REAL DEFAULT 0.0,
        t2_cash REAL DEFAULT 0.0,
        initial_cash REAL DEFAULT 0.0,
        nav_dirty_from_date TEXT
    );
    ''')

    # 2. 交易紀錄資料表
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS Trades (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        portfolio_id INTEGER NOT NULL,
        date TEXT NOT NULL,
        stock_id TEXT NOT NULL,
        action TEXT NOT NULL, -- Buy, Add, Reduce, Close
        price REAL NOT NULL,
        shares INTEGER NOT NULL,
        is_disposed BOOLEAN NOT NULL DEFAULT 0,
        basic_check BOOLEAN NOT NULL DEFAULT 0,
        tech_check BOOLEAN NOT NULL DEFAULT 0,
        chips_check BOOLEAN NOT NULL DEFAULT 0,
        industry_check BOOLEAN NOT NULL DEFAULT 0,
        trading_notes TEXT,
        settled INTEGER NOT NULL DEFAULT 0,
        technical_score INTEGER NOT NULL DEFAULT 3,
        chip_score INTEGER NOT NULL DEFAULT 3,
        theme_score INTEGER NOT NULL DEFAULT 3,
        take_profit_price REAL,
        stop_loss_price REAL,
        FOREIGN KEY (portfolio_id) REFERENCES Portfolios (id)
    );
    ''')

    # 3. 外部資金流動資料表 (出入金、配息)
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS CashFlows (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        portfolio_id INTEGER NOT NULL,
        date TEXT NOT NULL,
        type TEXT NOT NULL, -- Deposit, Withdrawal, Dividend
        amount REAL NOT NULL,
        FOREIGN KEY (portfolio_id) REFERENCES Portfolios (id)
    );
    ''')

    # 4. 總經大盤日誌資料表
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS MacroJournals (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        date TEXT NOT NULL UNIQUE,
        content TEXT NOT NULL
    );
    ''')

    # 5. 股票代號與名稱快取表
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS StockNames (
        stock_id TEXT PRIMARY KEY,
        name TEXT NOT NULL,
        full_symbol TEXT
    );
    ''')

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS PriceSnapshots (
        date TEXT NOT NULL,
        stock_id TEXT NOT NULL,
        close_price REAL NOT NULL,
        source TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        PRIMARY KEY (date, stock_id)
    );
    ''')

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS DailyNavSnapshots (
        portfolio_id INTEGER NOT NULL,
        date TEXT NOT NULL,
        nav REAL NOT NULL,
        cash REAL NOT NULL,
        holdings_value REAL NOT NULL,
        cash_flow REAL NOT NULL,
        daily_return REAL NOT NULL,
        twr REAL NOT NULL,
        benchmark_price REAL NOT NULL,
        benchmark_daily_return REAL NOT NULL,
        benchmark_cum_return REAL NOT NULL,
        daily_alpha REAL NOT NULL,
        cum_alpha REAL NOT NULL,
        drawdown REAL NOT NULL,
        notes TEXT,
        allocation_json TEXT,
        is_trading_day INTEGER NOT NULL DEFAULT 1,
        win_flag TEXT,
        updated_at TEXT NOT NULL,
        PRIMARY KEY (portfolio_id, date),
        FOREIGN KEY (portfolio_id) REFERENCES Portfolios (id)
    );
    ''')

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS TradeCycleReviews (
        portfolio_id INTEGER NOT NULL,
        stock_id TEXT NOT NULL,
        cycle_no INTEGER NOT NULL,
        review_text TEXT,
        updated_at TEXT NOT NULL,
        PRIMARY KEY (portfolio_id, stock_id, cycle_no),
        FOREIGN KEY (portfolio_id) REFERENCES Portfolios (id)
    );
    ''')

    # 寫入預設的測試資金池資料
    cursor.execute("INSERT INTO Portfolios (name, t0_cash, t2_cash, initial_cash) VALUES ('自己', 1000000.0, 1000000.0, 1000000.0)")
    cursor.execute("INSERT INTO Portfolios (name, t0_cash, t2_cash, initial_cash) VALUES ('爸爸', 500000.0, 500000.0, 500000.0)")

    conn.commit()
    conn.close()
    print("Database `trading_system.db` initialized successfully.")

if __name__ == '__main__':
    init_db()
