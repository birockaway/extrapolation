library(stats)
library(forecast)
devtools::install_github("twitter/AnomalyDetection")
library(AnomalyDetection)
library(chron)

# prevent scientific notation in numerics
options(scipen=999)

# DEVELOPEMENT ["devel"] / PRODUCTION ["prod"]?
# files in local are stored differently so you have to choose the enviroment mode
my_environment<-"prod"

# KBC PARAMETERS
if (my_environment=='devel') {
  #only for local use - in KBC the r-docker-application library is installed by default
  devtools::install_github('keboola/r-docker-application', ref = 'master')
  library(keboola.r.docker.application)
  # initialize application
  app <- keboola.r.docker.application::DockerApplication$new('in')
  app$readConfig()
} else {
  library(keboola.r.docker.application)
  # initialize application
  app <- keboola.r.docker.application::DockerApplication$new('/data/')
  app$readConfig()
}

### /FUNCTIONS DECLARATION SECTION ###

# END_OF_MONTH FCN
days_of_month_remain = function() {
  current_month<-(as.POSIXlt(Sys.Date()-1,tz='UTC'))$mon
  date_add<-as.POSIXlt(Sys.Date()-1,tz='UTC')
  i<-0
  while (date_add$mon==current_month){
    i<-i+1
    date_add<-as.POSIXlt(date_add+(60*60*24),tz='UTC')
  }
  return (i)
}

fill_the_dates=function(df){
  df<-df[as.POSIXct(df$date)>=as.POSIXct('2014-01-01'),]
  dates<-seq(as.POSIXct(min(df$date),tz='UTC'), as.POSIXct(Sys.Date()-1,tz='UTC'), "days")
  missing_index<-!(dates %in% df$date)
  
  fill_up<-df[1,]
  if (length(dates[missing_index])!=0){
    for (i in 1:length(dates[missing_index])){
      fill_up[i,]<-df[1,]
      fill_up[i,'date']<-dates[missing_index][i]
      fill_up[i,4:length(df)]<-rep(0,length(df)-3)  
    }
    df_filled<-rbind(df,fill_up)  
    df_filled<-df_filled[order(df_filled$date),]
  } else {df_filled<-df}
  return(df_filled)
}

positive_index=function(x) { if (x<1) {return(1)}else {return(x)}}

mkt_formating=function(df,sources_bridge) {
  df[,'sources_bridge_id']<-sapply(1:nrow(df),function(x) min(sources_bridge[sources_bridge[,'ForecastGroup']==df[x,'ForecastGroup'],'id']))
  
  return(df)  
}

remove_anomalies=function(mkt_ts,max_anoms=0.05,metric){
  # if the data is long enough
  if ((nrow(mkt_ts)-7)>14) {
    res = AnomalyDetectionTs(mkt_ts[1:(nrow(mkt_ts)-7),], max_anoms=max_anoms, direction='both', plot=T)
    #res
    
    #if some anomalies are found -> they will be replaced
    if (nrow(res$anoms)>0) {
      #finds anomalies' indexes
      anomaly_index<-sapply(1:nrow(res$anoms), function(x) which(res$anoms$timestamp[x]==mkt_ts$date))
      
      #replace anomalies with 14-days backward MA
      for (i in 1:length(anomaly_index)) {
        mkt_ts[anomaly_index[i],][,metric]<-mean(mkt_ts[positive_index((anomaly_index[i]-14)):positive_index((anomaly_index[i]-1)),][,metric])
      }
    }
    #compare plot
    #plot(mkt_data_src$date,mkt_data_src[,metric],type='l')
    #lines(mkt_data_src$date,mkt_data_src[,metric],type='l',col='red')
  }
  return(mkt_ts)
}

#Forecast fcn
forecast_this_month=function(mkt_data,metrics,web_id,ForecastGroup,ChannelType,in_frequency,anomalies=T,multi_seasonal=F,method='autoforecast',plot=F) {
  
  ### /DATASET HANDLING ###
  mkt_data_src<-mkt_data[mkt_data$ForecastGroup==ForecastGroup&mkt_data$web==web_id,]
  #check if there are any records on this metric/source
  if (nrow(mkt_data_src)==0) { return(mkt_data_src) }
  mkt_data_src_all<-fill_the_dates(mkt_data_src)
  
  forecast_df<-list()
  for (metric in metrics) {
    mkt_data_src<-mkt_data_src_all[,c('date',metric)]
    mkt_data_src<-mkt_data_src[order(mkt_data_src$date),]
    #plot(mkt_data_src$date,mkt_data_src[,metric],type='l')
    ### DATASET HANDLING/ ###
    
    
    #### /FORECAST SECTION ###
    # transfer to time series
    if ((nrow(mkt_data_src)/in_frequency)<=2) {in_frequency<-14}
    
    #plots
    #plot(decompose(mkt_data_src_ts))
    #plot(forecast(mkt_data_src_ts))
    
    #forecast
    if ((nrow(mkt_data_src)/in_frequency)<=2) {
      fcst_end_of_month_vals<-rep(mean(mkt_data_src[,metric]),days_of_month_remain())
    }else{
      options(warn=-1)
      if (multi_seasonal==T&metric!="cost"&((length(mkt_data_src[,metric])-min(which(mkt_data_src[,metric]>0)))>365*2)) {
        options(warn=0)
        mkt_data_src_ts<-msts(mkt_data_src[,metric],seasonal.periods = c(in_frequency,365.25),start=min(mkt_data_src[,"date"]))
        fit<-tbats(mkt_data_src_ts,use.damped.trend=T)
        if (plot==T) {plot(forecast(fit),main=paste("Two seasson Forecast -",metric))}
        fcst_end_of_month_vals<-forecast(fit,31*3)$mean
      } else {
        options(warn=0)
        ### /ANOMALY DETECTION ###
        if (anomalies==T) {mkt_data_src<-remove_anomalies(mkt_data_src,max_anoms=0.05,metric)}
        ### ANOMALY DETECTION/ ###
        if (method=='autoarima') {
          mkt_data_src<-mkt_data_src[mkt_data_src$date>as.POSIXct(Sys.Date()-120,tz='UTC'),]
          mkt_data_src_ts<-ts(mkt_data_src[,metric],frequency = in_frequency)
          fcst_end_of_month_vals<-forecast(auto.arima(mkt_data_src_ts),31*3)$mean
        } else {
          mkt_data_src<-mkt_data_src[mkt_data_src$date>as.POSIXct(Sys.Date()-120,tz='UTC'),]
          mkt_data_src_ts<-ts(mkt_data_src[,metric],frequency = in_frequency)
          fcst_end_of_month_vals<-forecast(mkt_data_src_ts,31*3)$mean
        }
      }
      for (i in (1:length(fcst_end_of_month_vals))){
        if (fcst_end_of_month_vals[i]<0) {fcst_end_of_month_vals[i]<-0}
      }
    }
    #### FORECAST SECTION/ ###
    
    max_date<-max(mkt_data_src_all[mkt_data_src_all$ForecastGroup==ForecastGroup,]$date)
    fcst_end_of_month_dates<-as.POSIXct(sapply(1:length(fcst_end_of_month_vals), function(x) as.POSIXlt(max_date,tz='UTC')+60*60*24*x),origin="1970-01-01",tz='UTC')
    
    
    sample<-mkt_data[mkt_data$ForecastGroup==ForecastGroup&mkt_data$web==web_id,][1,]
    forecast_df[[metric]]<-data.frame(ForecastGroup=rep(sample$ForecastGroup,length(fcst_end_of_month_vals)),web=rep(sample$web,length(fcst_end_of_month_vals)),date=fcst_end_of_month_dates)
    forecast_df[[metric]][,metric]<-fcst_end_of_month_vals
  }
  forecast_df_out<-forecast_df[[1]][,c('ForecastGroup','web','date')]
  for (metric in metrics) {
    forecast_df_out[,metric]<-round(abs(forecast_df[[metric]][,4]),4)
  }
  return(forecast_df_out)
}

# select batch of web_ids on the basis of config parameters (eg. second 1/6 of webids)
select_web_ids<-function(parameters,structure) {
  web_batch<-parameters$web_batch
  if (parameters$metric_type=='session_granularity'){
    web_ids_all<-structure[structure$division=='all'&!is.na(structure$division)&structure$status=='on',"pk"]
    step<-ceiling(length(web_ids_all)/web_batch$denominator)
    
    web_ids<-unlist(lapply(web_batch$numerator,function(numerator) web_ids_all[((numerator-1)*step+1):(min(length(web_ids_all),(numerator)*step))]))
  }
  return(web_ids)
}

### FUNCTIONS DECLARATION SECTION/ ###




### /DATASET HANDLING ###
sources_bridge<-read.csv("in/tables/sources_bridge.csv",stringsAsFactors = F)
structure<-read.csv("in/tables/structure.csv",stringsAsFactors = F)
mkt_data_original<-read.csv("in/tables/extrapolation_ini.csv",stringsAsFactors = F)
mkt_data<-mkt_data_original
mkt_data$date<-as.POSIXct(mkt_data$date,tz='UTC')
mkt_data<-mkt_data[!is.na(mkt_data$ForecastGroup),]
#mkt_data$ForecastGroup<-sapply(1:nrow(mkt_data), function(x) sources_bridge[sources_bridge$id==(mkt_data[x,"sources_bridge_id"]),"ForecastGroup"])
### DATASET HANDLING/ ###

#results
mkt_data_out<-mkt_data
#mkt_data_out2<-mkt_data

ForecastGroups<-unique(mkt_data$ForecastGroup)
#web_ids<-unique(mkt_data$web)
# access the supplied value of 'web'

web_ids<-select_web_ids(app$getParameters(),structure)

# cut data to the current day
mkt_data<-mkt_data[mkt_data$date<as.POSIXct(Sys.Date(),tz='UTC'),]
#ChannelTypes<-unique(sources_bridge$ChannelType)
#ForecastGroup<-ForecastGroups[1]
#web_id<-web_ids[3]

for (web_id in web_ids) {
  print(paste("Web_id",web_id,"ready to forecast..."))
  for (ForecastGroup in ForecastGroups) {
    ChannelType<-sources_bridge[sources_bridge$ForecastGroup==ForecastGroup,'ChannelType'][1]
    #undebug(forecast_this_month)
    forecast<-forecast_this_month(mkt_data,c('sessions','transactions','transactionrevenue','cost'),web_id,ForecastGroup,ChannelType,in_frequency=7,anomalies=T,multi_seasonal =T,plot=F)
    
    #compare plot
    metrics<-c('cost','sessions')
    mkt_data_src<-mkt_data[mkt_data$ForecastGroup==ForecastGroup,]
    mkt_data_src<-mkt_data_src[order(mkt_data_src$date),]
    do_plot<-T
    #mkt_data_src<-rbind(mkt_data_src[,c('ForecastGroup','web','date',metric)],forecast2[,c('ForecastGroup','web','date',metric)])
    if (nrow(mkt_data_src[mkt_data_src$web==web_id,])>0 & do_plot==T) {
      #plot prvni metriky
      mkt_data_src<-fill_the_dates(mkt_data_src[mkt_data_src$web==web_id,])
      plot(c(mkt_data_src[mkt_data_src$web==web_id,metrics[1]],rep(NA,nrow(forecast))),type='l',main=ForecastGroup,ylim=c(min(min(mkt_data_src[,metrics[1]]),min(mkt_data_src[,metrics[2]])),max(max(mkt_data_src[,metrics[1]]),max(mkt_data_src[,metrics[2]]))))
      lines(c(rep(NA,nrow(mkt_data_src[mkt_data_src$web==web_id,])),forecast[,metrics[1]]),type='l',col='red')
      #lines(c(rep(NA,nrow(mkt_data_src[mkt_data_src$web==web_id,])),forecast2[,metrics[1]]),type='l',col='green')
      #plot druhe metriky
      lines(c(mkt_data_src[mkt_data_src$web==web_id,metrics[2]],rep(NA,nrow(forecast))),type='l',lty=2,main=ForecastGroup)
      lines(c(rep(NA,nrow(mkt_data_src[mkt_data_src$web==web_id,])),forecast[,metrics[2]]),type='l',lty=2,col='blue')
      #lines(c(rep(NA,nrow(mkt_data_src[mkt_data_src$web==web_id,])),forecast2[,metrics[2]]),type='l',lty=2,col='green')
    }
    
    
    #bind dataset
    mkt_data_out<-rbind(mkt_data_out,forecast)
    #mkt_data_out2<-rbind(mkt_data_out2,forecast2)
  }
  print(paste("Web_id",web_id,"processed successfuly!"))
}

#doplni sources_bridge_ids - nahodne, pouze aby se to v GD naparovalo na spravny ForecastGroups
#undebug(mkt_formating)
mkt_data_out<-mkt_formating(mkt_data_out,sources_bridge)


#snapshotting on specific days of month
snapshotting_days<-c(1,10,20,30)
if (as.numeric(days(Sys.Date()))%in%snapshotting_days) {
  mkt_data_out_snap<-mkt_data_out
  mkt_data_out_snap[,"snap_date"]<-Sys.Date()
} else {
  mkt_data_out_snap<-mkt_data_out[numeric(0),]
  mkt_data_out_snap[,"snap_date"]<-numeric(0)
}

#write.csv(mkt_data_out_snap,file = "out/tables/extrapolation_out_snap.csv", row.names = FALSE)
write.csv(mkt_data_out,file = "out/tables/extrapolation_out.csv", row.names = FALSE)
