# TWSE_ShortQuotaScrape Project
## Description
Real time handling and alert system for TWSE Short Quota page <br>
Url: https://mis.twse.com.tw/stock/sblInquiryCap.jsp?lang=en_use&oddFlag=undefined#

### Basic info about TWSE
- TWSE Trading Hours:
  - Morning market: 9:00 am to 1:30pm
  - Afternoon market: 
- Order placing hours:
  - 8:30-13:30
- Short quota
  - TWSE only has a limited short sell quota for every stock
  - Cannot place short order when quota runs out of the day
  - Notice

### TWSE Shorts Concepts
- [SBL Disclosures](https://www.twse.com.tw/en/page/trading/SBL/t13sa710.html): Can see the fee rate and number of SBL trades volume
- 借券賣出餘額 Definition?
  - 本日借券賣出數量加計前一日尚未回補的數額再減去本日借券賣出回補數量後的數額
- 借券餘額 Definition?
  - 代表借入證券尚未返還的數額
  - 因為外資將資金匯入台灣後，必須在一定時限內投入市場，但有些時候股價並不在一個理想的價位，因此外資有時候會先利用借券的方式暫時將資金投入市場，而相關成本只有約定的借券利息而已。

### Other rules:
- Reference price of certain stocks rises or falls more than 3.5 percent of the last reference price one minute before the close (1:29 pm-1:30 pm), investors are allowed to add, modify or cancel their orders of that stock from 1:31 pm to 1:33 pm.
- 

### Task requirement
- Scrape the TWSE short sell quota
- Design an alert system on situations you consider necessary
- You can decide what parameters to use, but need to state your assumption.
- Elements in report:
  - Explain findings
  - Analysis on significance of this real time update and its implication on daily trading work
  - Explain your methodology on scraping the data
  - How would you design the alert system to detect significant event
- Optional elements
  - Pair the result with news to explain fundamental reasons
  - Trading ideas around the info
  - Knowledge around TWSE Micro-structure
  - Your struggles in working on the project
