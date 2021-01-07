-- CREATE DATABASE prism;
-- use prism;

CREATE TABLE user (
    userName VARCHAR(30) PRIMARY KEY,
    uid INT UNSIGNED AUTO_INCREMENT,
    INDEX (uid)
)AUTO_INCREMENT = 0;

-- For record all trading date.
CREATE TABLE tradingDay (
    date DATE PRIMARY KEY
);

-- For T0 trading delta.
CREATE TABLE parrot (
    uid INT UNSIGNED,
    date DATE,
    symbol VARCHAR(10),
    volumeDelta INT,
    PRIMARY KEY(uid, date, symbol),
    FOREIGN KEY (uid) REFERENCES user(uid) ON DELETE CASCADE
);

DELIMITER $$
CREATE TRIGGER update_trading_day1 BEFORE INSERT ON parrot
FOR EACH ROW
BEGIN
INSERT IGNORE INTO tradingDay (date) VALUES (NEW.date);
END$$
DELIMITER ;

-- For T2 trading delta.
CREATE TABLE pigeon (
    uid INT UNSIGNED,
    date DATE,
    symbol VARCHAR(10),
    volumeDelta INT,
    PRIMARY KEY(uid, date, symbol),
    FOREIGN KEY (uid) REFERENCES user(uid) ON DELETE CASCADE
);

DELIMITER $$
CREATE TRIGGER update_trading_day2 BEFORE INSERT ON pigeon
FOR EACH ROW
BEGIN
INSERT IGNORE INTO tradingDay (date) VALUES (NEW.date);
END$$
DELIMITER ;

CREATE TABLE ostrich (
    uid INT UNSIGNED,
    date DATE,
    symbol VARCHAR(10),
    volumeDelta INT,
    PRIMARY KEY(uid, date, symbol),
    FOREIGN KEY (uid) REFERENCES user(uid) ON DELETE CASCADE
);

-- For T0 trading EOD volume.
CREATE TABLE eod_parrot (
    uid INT UNSIGNED,
    date DATE,
    symbol VARCHAR(10),
    volume INT,
    PRIMARY KEY(uid, date, symbol),
    FOREIGN KEY (uid) REFERENCES user(uid) ON DELETE CASCADE
);

-- For T2 trading EOD volume.
CREATE TABLE eod_pigeon (
    uid INT UNSIGNED,
    date DATE,
    symbol VARCHAR(10),
    volume INT,
    PRIMARY KEY(uid, date, symbol),
    FOREIGN KEY (uid) REFERENCES user(uid) ON DELETE CASCADE
);