Parameterfile=read.csv("C:/Users/e0013353/Desktop/Parmeter/DATA/ParameterFile.csv",header = TRUE)
library(sqldf)
library(RODBCext)
connHandle <-odbcConnect("**s",uid = "root",pwd = "root")
dataf<-data.frame()
for(i in 1:nrow(Parameterfile))   
{
  unitid<-Parameterfile[i,3]
  maindirct<-Parameterfile[i,4]
  query<-paste0("select distinct * from ARCHIVE.testdb3 where date_time >= '",Parameterfile[i,1],"' and date_time < '",Parameterfile[i,2],"' and unitid='",Parameterfile[i,3],"'")
  Data<-sqlQuery(connHandle ,query)
  Data$Id<-NULL
  fileid<-unitid
  path<-Createdirectory(maindirct,fileid)
  dataf[i,1]<-path
  filespace<-sprintf(paste0(path,"/%s.csv"),unitid)
  write.csv(Data,filespace,row.names = FALSE)
  if(i == nrow(Parameterfile))
  {
    library(mailR)
    from<-"seshan1490@gmail.com" ## From email address##
    to<-c("e0013353@u.nus.edu")  ## To email address##
    send.mail(from = from,
              to = to,
              subject="Data Extraction Process has been Completed",
              body = "Hello Team, 
              The Data Extraction from Database has been successfully completed and files has been placed in the folders",
              smtp = list(host.name = "smtp.gmail.com", port = 465,user.name="**********@gmail.com",passwd="********",ssl=TRUE),
              authenticate = TRUE,
              send = TRUE)
    Paramfile=read.csv("C:/Users/e0013353/Desktop/Parmeter/Outlier/ParameterFile.csv",header = TRUE)##To Read the paremeter file##
    
    dfFinalCleanRecords <- NULL
    for(j in 1:nrow(dataf))
    {
      CleanRecords <- NULL  
      sdf <- dataf[j,1]
      for(i in 1:nrow(Paramfile))   
      {
        filename<-Paramfile[i,1]  
        columname<-Paramfile[i,2] 
        x<-Paramfile[i,3]      
        y<-Paramfile[i,4]     
        unid<-Paramfile[i,5]
        Omaindirct<-Paramfile[1,6]
        Cmaindirct<-Paramfile[1,7]
        Sfilename=toString(filename)
        Scolumname=(columname)
        folderDepth <- length(strsplit(sdf,c("/"))[[1]])
        if(trimws(strsplit(strsplit(sdf,c("/"))[[1]][folderDepth],c("_"))[[1]][1])==unid)
        {
          dfFinalOutput <- getOutlierRecordsFromfile(Sfilename,Scolumname,x,y,unid,sdf)
          colname<-as.data.frame(dfFinalOutput[1])
          ##CleanRecords <- as.data.frame(dfFinalOutput[2])
          ##CleanRecord<- CleanRecords
          Outlierfile<-as.data.frame(dfFinalOutput[3])
          ##dfoutfilename=paste(unid,"_",colname[1,1])
          library(sqldf)
          library(RODBCext)
          connHandle <-odbcConnect("ses",uid = "root",pwd = "root")
          for(i in 1:nrow(Outlierfile))
          {
            sqlQuery(connHandle,paste("insert into archive.",as.character(colname[1,1])," (unitid,noise_value,light_value,temp_value,co2_value,voc_value,humid_value,date_time,Flag)
                                      values('",as.character(Outlierfile$unitid[i]),"',",Outlierfile$noise_value[i],",",Outlierfile$light_value[i],",",Outlierfile$temp_value[i],",",Outlierfile$co2_value[i],",",Outlierfile$voc_value[i],",",Outlierfile$humid_value[i],",'",Outlierfile$date_time[1],"','",as.character(Outlierfile$Flag[i]),"')", sep=""))
          }  
          close(connHandle)
          # if(columname==2)
          # {
          #   opath<-outlierdirectory(Omaindirct,unid)
          #   filespace<-sprintf(paste0(opath,"/%s.csv"),dfoutfilename)
          #   write.csv(Outlierfile,filespace,row.names = FALSE)
          # }else{filespace<-sprintf(paste0(opath,"/%s.csv"),dfoutfilename)
          # write.csv(Outlierfile,filespace,row.names = FALSE)}
          if(is.null(CleanRecords))
          {
            CleanRecords<-as.data.frame(dfFinalOutput[2])
            CleanRecord<- CleanRecords
          }else{CleanRecord<-merge(CleanRecord,as.data.frame(dfFinalOutput[2]),by ="Date_time")
          CleanRecord$unitid.y=NULL
          CleanRecord$unitid=NULL
          }
        }
      }
      dfFinalCleanRecords <- rbind(dfFinalCleanRecords,CleanRecord)
      cpath<-cleandirectory(Cmaindirct)
      filespace<-sprintf(paste0(cpath,"/%s.csv"),trimws(strsplit(strsplit(sdf,c("/"))[[1]][folderDepth],c("_"))[[1]][1]))
      write.csv(dfFinalCleanRecords,filespace,row.names = FALSE)
      library(sqldf)
      library(RODBCext)
      connHandle <-odbcConnect("ses",uid = "root",pwd = "root")
      for(i in 1:nrow(dfFinalCleanRecords))
      {
        sqlQuery(connHandle,paste("insert into archive.cleandata (Date_time,unitid,noise_value,light_value,temp_value,co2_value,voc_value,humid_value)
                                  values('",dfFinalCleanRecords$Date_time[i],"','",as.character(dfFinalCleanRecords$unitid[i]),"','",dfFinalCleanRecords$noise_value[i],"','",dfFinalCleanRecords$light_value[i],"','",dfFinalCleanRecords$temp_value[i],"','",dfFinalCleanRecords$co2_value[i],"','",dfFinalCleanRecords$voc_value[i],"','",dfFinalCleanRecords$humid_value[i],"')", sep=""))
      }  
      close(connHandle)
      dfFinalCleanRecords <- NULL
      if(j == nrow(dataf))
      {  
        send.mail(from = from,
                  to = to,
                  subject="Outlier Process has been completed Successfuly",
                  body = "Hello Team, 
                  Outlier Process has been completed Successfuly and file has been placed in the respective folders",
                  smtp = list(host.name = "smtp.gmail.com", port = 465,user.name="seshan1490@gmail.com",passwd="Varadan@64",ssl=TRUE),
                  authenticate = TRUE,
                  send = TRUE)
        Parfile=read.csv("C:/Users/e0013353/Desktop/Parmeter/Aggregation/ParameterFile.csv",header = TRUE)
        for(i in 1:nrow(Parfile))
        {
          Afilename<-Parfile[i,1]  
          Acolumname<-Parfile[i,2] 
          MiutesToTranform<-Parfile[i,3] 
          id<-Parfile[i,4]
          zone<-Parfile[i,5]
          Level<-Parfile[i,6]
          Stfilename=toString(Afilename)   
          Stcolumname=(Acolumname)      
          MiutoAvg=MiutesToTranform  
          Aggreagatedfile <- Aggregatingfile(Stfilename,Stcolumname,MiutoAvg,id,zone,Level)
        }
      }
    }
  }
}
##################################################################################################
Aggregatingfile<-function(Stfilename,Stcolumname,MiutoAvg,id,zone,Level)  
{
  library(xts) 
  library(zoo)
  library(lubridate)
  file_dict=setwd(cpath) 
  Afile=read.csv(Stfilename,header= TRUE,stringsAsFactors=FALSE)  
  file=na.omit(Afile)
  file_xts=xts(file,order.by=as.POSIXct(file[,Stcolumname],format="%Y-%m-%d %H:%M"))
  file_xts$Date_time=NULL 
  ep=endpoints(file_xts,on="minutes",k=MiutoAvg) 
  Aggregate=period.apply(file_xts,INDEX=ep,FUN=mean)  
  df=as.data.frame(Aggregate)                   
  Date_time=index(Aggregate)    
  Date_time=as.character(as.POSIXct(Date_time)) 
  dfDate_time=as.data.frame(Date_time)
  Combinefile=cbind(df,dfDate_time)
  Combinefile$unitid.x<-id
  Combinefile$Days<-wday(Combinefile$Date_time, label = TRUE)
  Combinefile$Weekday<-NA
  Combinefile$Weekday=as.factor(ifelse(Combinefile$Days=='Mon'| Combinefile$Days=='Tue'| Combinefile$Days=='Wed'| Combinefile$Days=='Thu'| Combinefile$Days=='Fri',"Weekday","Weekend"))
  Combinefile$Start_flag<-NA
  Combinefile$End_flag<-NA
  Combinefile$Zone<-zone
  Combinefile$Level<-Level
  Max_value=max(Combinefile$light_value)
  Min_value=min(Combinefile$light_value)
  Avg_min_max=(Max_value+Min_value)
  setpoint=( Avg_min_max/2)
  for(i in 1:(length(Combinefile$light_value)-1))
  {
    tail(Combinefile)
    Start_time<-(Combinefile$light_value[i] < setpoint & Combinefile$light_value[i+1] > setpoint)
    Combinefile$Start_flag[i+1]<-Start_time
    End_time<-(Combinefile$light_value[i] > setpoint & Combinefile$light_value[i+1] < setpoint)
    Combinefile$End_flag[i+1]<-End_time
    Combinefile$Date_time<-as.character(Combinefile$Date_time)
    Start_End_Time<-subset(Combinefile,Combinefile$Start_flag ==TRUE | Combinefile$End_flag ==TRUE,select=1:ncol(Combinefile))
    Split_Record<- subset(Combinefile,Combinefile$Date_time >= (Start_End_Time[1,8]) & Combinefile$Date_time <= (Start_End_Time[2,8]))
    Combinefile$Hours[Combinefile$Date_time %in% Split_Record$Date_time] <- "OFFICEHOURS"
    if(i==(length(Combinefile$light_value)-1))
  {
    colnames(Combinefile)[1]<-"unitid"
    Combinefile$Start_flag<-NULL
    Combinefile$End_flag<-NULL
      if(MiutoAvg==5)
      {
        library(sqldf)
        library(RODBCext)
        connHandle <-odbcConnect("ses",uid = "root",pwd = "root")
        for(i in 1:nrow(Combinefile))
        {
        sqlQuery(connHandle,paste("insert into archive.fivemindata (unitid,noise_value,light_value,temp_value,co2_value,voc_value,humid_value,Date_time,Days,Weekday,Zone,Level,Hours)
         values('",as.character(Combinefile$unitid[i]),"','",Combinefile$noise_value[i],"','",Combinefile$light_value[i],"','",Combinefile$temp_value[i],"','",Combinefile$co2_value[i],"','",Combinefile$voc_value[i],"','",Combinefile$humid_value[i],"','",Combinefile$Date_time[i],"','",as.character(Combinefile$Days[i]),"','",as.character(Combinefile$Weekday[i]),"','",as.character(Combinefile$Zone[i]),"','",Combinefile$Level[i],"','",Combinefile$Hours[i],"')", sep=""))
        }  
        close(connHandle)
      }
      else if(MiutoAvg==15)
      {
        library(sqldf)
        library(RODBCext)
        connHandle <-odbcConnect("ses",uid = "root",pwd = "root")
        for(i in 1:nrow(Combinefile))
        {
          sqlQuery(connHandle,paste("insert into archive.fifteenmindata (unitid,noise_value,light_value,temp_value,co2_value,voc_value,humid_value,Date_time,Days,Weekday,Zone,Level,Hours)
                                    values('",as.character(Combinefile$unitid[i]),"','",Combinefile$noise_value[i],"','",Combinefile$light_value[i],"','",Combinefile$temp_value[i],"','",Combinefile$co2_value[i],"','",Combinefile$voc_value[i],"','",Combinefile$humid_value[i],"','",Combinefile$Date_time[i],"','",as.character(Combinefile$Days[i]),"','",as.character(Combinefile$Weekday[i]),"','",as.character(Combinefile$Zone[i]),"','",Combinefile$Level[i],"','",Combinefile$Hours[i],"')", sep=""))
        }  
        close(connHandle)
      }
      
      else if(MiutoAvg==30)
      {
        library(sqldf)
        library(RODBCext)
        connHandle <-odbcConnect("ses",uid = "root",pwd = "root")
        for(i in 1:nrow(Combinefile))
        {
          sqlQuery(connHandle,paste("insert into archive.thirtymindata (unitid,noise_value,light_value,temp_value,co2_value,voc_value,humid_value,Date_time,Days,Weekday,Zone,Level,Hours)
                                    values('",as.character(Combinefile$unitid[i]),"','",Combinefile$noise_value[i],"','",Combinefile$light_value[i],"','",Combinefile$temp_value[i],"','",Combinefile$co2_value[i],"','",Combinefile$voc_value[i],"','",Combinefile$humid_value[i],"','",Combinefile$Date_time[i],"','",as.character(Combinefile$Days[i]),"','",as.character(Combinefile$Weekday[i]),"','",as.character(Combinefile$Zone[i]),"','",Combinefile$Level[i],"','",Combinefile$Hours[i],"')", sep=""))
         
        }  
        close(connHandle)
      }
      
      else if(MiutoAvg==60)
      {
        library(sqldf)
        library(RODBCext)
        connHandle <-odbcConnect("ses",uid = "root",pwd = "root")
        for(i in 1:nrow(Combinefile))
        {
          sqlQuery(connHandle,paste("insert into archive.sixtymindata (unitid,noise_value,light_value,temp_value,co2_value,voc_value,humid_value,Date_time,Days,Weekday,Zone,Level,Hours)
                                    values('",as.character(Combinefile$unitid[i]),"','",Combinefile$noise_value[i],"','",Combinefile$light_value[i],"','",Combinefile$temp_value[i],"','",Combinefile$co2_value[i],"','",Combinefile$voc_value[i],"','",Combinefile$humid_value[i],"','",Combinefile$Date_time[i],"','",as.character(Combinefile$Days[i]),"','",as.character(Combinefile$Weekday[i]),"','",as.character(Combinefile$Zone[i]),"','",Combinefile$Level[i],"','",Combinefile$Hours[i],"')", sep=""))
        
        }  
        close(connHandle)
      }
      else (MiutoAvg==1440)
    {
      library(sqldf)
      library(RODBCext)
      connHandle <-odbcConnect("ses",uid = "root",pwd = "root")
      Combinefile=Combinefile[-1,]
      for(i in 1:nrow(Combinefile))
      {
        sqlQuery(connHandle,paste("insert into archive.onedaydata (unitid,noise_value,light_value,temp_value,co2_value,voc_value,humid_value,Date_time,Days,Weekday,Zone,Level,Hours)
                                  values('",as.character(Combinefile$unitid[i]),"','",Combinefile$noise_value[i],"','",Combinefile$light_value[i],"','",Combinefile$temp_value[i],"','",Combinefile$co2_value[i],"','",Combinefile$voc_value[i],"','",Combinefile$humid_value[i],"','",Combinefile$Date_time[i],"','",as.character(Combinefile$Days[i]),"','",as.character(Combinefile$Weekday[i]),"','",as.character(Combinefile$Zone[i]),"','",Combinefile$Level[i],"','",Combinefile$Hours[i],"')", sep=""))
      }  
      close(connHandle)
      }                  
    }
  }
}
###########################################################################################################
getOutlierRecordsFromfile<-function(Sfilename,Scolumname,x,y,unid,sdf) ##function to flag Outliers in file###
{
  library(zoo)
  ##file_dict=setwd("C:/Users/e0013353/Desktop/input/Files")  ## To set the file Directory##
  file_dict=setwd(sdf)
  file=read.csv(Sfilename)                      ## To read the file given file ##
  summary(file)                                 ## To to get summary of file ##
  UpperOutlierRange=x                          ##Assigning user given upperoutlier range##
  LowerOutlierRange=y                          ##Assigning user given loweroutlier rage##
  file$Flag<-NA                                ##To create a new flag column###
  colname=colnames(file[Scolumname])           ##To get column name ##
  medianValue=median(file[,Scolumname])        ##To get the median value ####
  boxplot(file[,Scolumname], main="BOX PLOT", boxwex=0.1,xlab=colname,ylab="Range") ##To draw a box plot##
  Upper_Outlier <- subset(file, file[,Scolumname] > UpperOutlierRange, select = 1:ncol(file)) ##To find the value greater than upper outlier value###
  lower_Outlier <- subset(file, file[,Scolumname] < LowerOutlierRange, select = 1:ncol(file)) ##To find the value less than lower outlier value##
  AllOutlierRecords <- rbind(lower_Outlier,Upper_Outlier)   ### To bind the upper and lower outlier records###
  CleanRecords <- subset(file,!(file[,Scolumname] %in% AllOutlierRecords[,Scolumname]),select=c(8,1,Scolumname))##To remove the outlier records from the normal  data##
  CleanRecords$date_time=as.POSIXct(CleanRecords$date_time,format='%Y-%m-%d %H:%M:%S')
  mfile1<-zoo(CleanRecords[,-1],CleanRecords[,1])
  mfile2 <- merge(mfile1,zoo(seq(start(mfile1),end(mfile1),by="10 secs")), all=TRUE)
  df=as.data.frame(mfile2)                         
  Date_time=index(mfile2)
  Date_time=as.character(as.POSIXct(Date_time))
  dfDate_time=as.data.frame(Date_time)
  Combinefile=cbind(df,dfDate_time)
  file$Flag[file[,Scolumname] %in% AllOutlierRecords[,Scolumname]] <- "outlier" ##To flag the values###
  return(list(colname,Combinefile,file))
}

################################################################################################################
Createdirectory<-function(maindirct,fileid)
{
  CurrentDate<-Sys.Date()
  folder=paste(fileid,"_",CurrentDate)
  filepath=file.path(maindirct,folder)
  Createfolder=dir.create(filepath,'0777')
  return(filepath)
}
################################################################################
outlierdirectory<-function(Omaindirct,fileid)
{
  CurrentDate<-Sys.Date()
  folder=paste(fileid,"_",CurrentDate)
  filepath=file.path(Omaindirct,folder)
  Createfolder=dir.create(filepath,'0777')
  return(filepath)
}
###############################################################################
cleandirectory<-function(Cmaindirct)
{
  CurrentDate<-Sys.Date()
  folder=paste("CleanData","_",CurrentDate)
  filepath=file.path(Cmaindirct,folder)
  Createfolder=dir.create(filepath,'0777')
  return(filepath)
}
################################################################################
Aggredirectory<-function(Omaindirct)
{
  CurrentDate<-Sys.Date()
  folder=paste("Files","_",CurrentDate)
  filepath=file.path(Omaindirct,folder)
  Createfolder=dir.create(filepath,'0777')
  return(filepath)
}
