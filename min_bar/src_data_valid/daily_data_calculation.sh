#!/bin/sh -e

# find target date
function __readINI() {
  INIFILE=$1; SECTION=$2;	ITEM=$3
  _readIni=`awk -F '=' '/\['$SECTION'\]/{a=1}a==1&&$1~/'$ITEM'/{print $2;exit}' $INIFILE`
  echo ${_readIni}
}

use_history=$( __readINI target_date.ini set calculate_hist)
if [[ $use_history == *'True'* ]]
then
  target_date=$( __readINI target_date.ini set target_date)
  echo "Use History Date ${target_date}"
else
  target_date=$(date +%Y%m%d)
  echo "Use Today Date ${target_date}"
fi

#输出文件路径
OUTDIR=/home/zhoutw/daily_job/after_market_output/${target_date}
mkdir -p ${OUTDIR}

cd /home/zhoutw/daily_job/check_data
/root/anaconda3/bin/python3 valid_data.py &> ${OUTDIR}/check_data_log.out

if grep -q True /home/zhoutw/daily_job/after_market_output/${target_date}/${target_date}.ini
then
    cd /home/zhoutw/daily_job
    echo "snapshot turnover and wind turnover paired"
    echo "start to calculate after market data"
    ./after_market_close.sh
else
    echo "snapshot turnover and wind turnover not paired"
    echo "Exit and failed"
fi
