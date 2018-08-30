Parameterfile=read.csv("C:/Users/e0013353/Desktop/Parmeter/DATA/ParameterFile.csv",header = TRUE) ##To Read the input value for data extraction from  parameter file and store the value in Parameterfile Variable####
library(sqldf) 
library(RODBCext)
connHandle <-odbcConnect("ses",uid = "root",pwd = "root")  ##To Store the database connection details in connhandle variable#####
dataf<-data.frame() ##To Create a empty dataframe and storing it in dataf###
for(i in 1:nrow(Parameterfile)) ###For loop for reading the input value from parameter file row by row and  processing it####  
{
  unitid<-Parameterfile[i,3]  ###To Read third column value(Unit id) from parameter file and strore in unitid variable##
  maindirct<-Parameterfile[i,4] ###To Read fourth column value(File path) from parameter file and strore in  maindirct variable##
  query<-paste0("select distinct * from ARCHIVE.testdb3 where date_time >= '",Parameterfile[i,1],"' and date_time < '",Parameterfile[i,2],"' and unitid='",Parameterfile[i,3],"'") ##Select query with start data,end date and unitid value taken from parameter file and storing the value in query variable##
  Data<-sqlQuery(connHandle ,query) ##Execute the select statement to fetch data from database and storing the value in Data Variable##
  Data$Id<-NULL ### To Remove the id column from Data dataframe###
  fileid<-unitid ### To store the unitid value in fileid### 
  path<-Createdirectory(maindirct,fileid) ###To Create a path where the file need to be saved ##
  dataf[i,1]<-path ##To store the path value in dataframe dataf##
  filespace<-sprintf(paste0(path,"/%s.csv"),unitid)
  write.csv(Data,filespace,row.names = FALSE) ###To save the file in the given path ####
  if(i == nrow(Parameterfile)) ##If for loop value is equal to last row of parameter file then it will enter the loop##
  {
    library(mailR)
    from<-"seshan1490@gmail.com" ## From email address##
    to<-c("e0013353@u.nus.edu")  ## To email address##
    send.mail(from = from,
              to = to,
              subject="Data Extraction Process has been Completed",
              body = "Hello Team, 
              The Data Extraction from Database has been successfully completed and files has been placed in the folders",
              smtp = list(host.name = "smtp.gmail.com", port = 465,user.name="seshan1490@gmail.com",passwd="Varadan@64",ssl=TRUE),##To define the email address, password and connection###
              authenticate = TRUE,
              send = TRUE)  ### To Send an email to acknowledge the data extraction process completion###
    Paramfile=read.csv("C:/Users/e0013353/Desktop/Parmeter/Outlier/ParameterFile.csv",header = TRUE)##To Read input value for outlier analysis from the Outlier parameter file##
    
    dfFinalCleanRecords <- NULL ###To clear the values in  dfFinalCleanRecords dataframe ##
    for(j in 1:nrow(dataf))     ## For loop for reading the value stored in dataf and processing it row by row####
    {
      CleanRecords <- NULL      ##To clear the values in CleanRecords dataframe ##
      sdf <- dataf[j,1]         ##TO read the first column value from dataframe dataf and storing the value in sdf variable##
      for(i in 1:nrow(Paramfile)) ##To read the values from Outlier paramfile row by row and process it##
      {
        filename<-Paramfile[i,1]  ## To Read the value from first column of paramfile##
        columname<-Paramfile[i,2]  ## To Read the value from second column of paramfile##
        x<-Paramfile[i,3]           ## To Read the value from third column of paramfile##
        y<-Paramfile[i,4]            ## To Read the value from fourth column of paramfile##
        unid<-Paramfile[i,5]          ## To Read the value from fifth column of paramfile##
        Omaindirct<-Paramfile[1,6]     ## To Read the value from sixth column of paramfile##
        Cmaindirct<-Paramfile[1,7]      ## To Read the value from seventh column of paramfile##
        Sfilename=toString(filename)    ## To convert the datatype to string ##
        Scolumname=(columname)  
        folderDepth <- length(strsplit(sdf,c("/"))[[1]]) 
        if(trimws(strsplit(strsplit(sdf,c("/"))[[1]][folderDepth],c("_"))[[1]][1])==unid) ##To match the unitid of the sensors and the file name##
        {
          dfFinalOutput <- getOutlierRecordsFromfile(Sfilename,Scolumname,x,y,unid,sdf)##To call the function dfFinalOutput##
          colname<-as.data.frame(dfFinalOutput[1]) ##To Store the value returned from the function###
          ##CleanRecords <- as.data.frame(dfFinalOutput[2])
          ##CleanRecord<- CleanRecords
          Outlierfile<-as.data.frame(dfFinalOutput[3])##To store outlier value returned from the function ##
          ##dfoutfilename=paste(unid,"_",colname[1,1])
          library(sqldf)   
          library(RODBCext)
          connHandle <-odbcConnect("ses",uid = "root",pwd = "root")   ##To Store the database connection details in connhandle variable#####
          for(i in 1:nrow(Outlierfile))  
          {
            sqlQuery(connHandle,paste("insert into archive.",as.character(colname[1,1])," (unitid,noise_value,light_value,temp_value,co2_value,voc_value,humid_value,date_time,Flag)
                                      values('",as.character(Outlierfile$unitid[i]),"',",Outlierfile$noise_value[i],",",Outlierfile$light_value[i],",",Outlierfile$temp_value[i],",",Outlierfile$co2_value[i],",",Outlierfile$voc_value[i],",",Outlierfile$humid_value[i],",'",Outlierfile$date_time[1],"','",as.character(Outlierfile$Flag[i]),"')", sep=""))
          }  ##To insert the value into rhe database##
          close(connHandle) ##Disconnecting the connection between database and R##
          # if(columname==2)
          # {
          #   opath<-outlierdirectory(Omaindirct,unid)
          #   filespace<-sprintf(paste0(opath,"/%s.csv"),dfoutfilename)
          #   write.csv(Outlierfile,filespace,row.names = FALSE)
          # }else{filespace<-sprintf(paste0(opath,"/%s.csv"),dfoutfilename)
          # write.csv(Outlierfile,filespace,row.names = FALSE)}
          if(is.null(CleanRecords))  ##To check is cleanRecords dataframe is empty##
          {
            CleanRecords<-as.data.frame(dfFinalOutput[2]) ##To store CleanData(i.e. after outlier removal) returned from function ##
            CleanRecord<- CleanRecords  ##To store the values in CleanRecord dataframe
          }else{CleanRecord<-merge(CleanRecord,as.data.frame(dfFinalOutput[2]),by ="Date_time") ##To merge the multiple file into a single file##
          CleanRecord$unitid.y=NULL ##To remove the unitid.y columns##
          CleanRecord$unitid=NULL
          }
          }
      }
      dfFinalCleanRecords <- rbind(dfFinalCleanRecords,CleanRecord) ##To merge the clean record dataframe which consist of clean record into single dataframe##
      cpath<-cleandirectory(Cmaindirct) ##To store the path where the file has to be saved##
      filespace<-sprintf(paste0(cpath,"/%s.csv"),trimws(strsplit(strsplit(sdf,c("/"))[[1]][folderDepth],c("_"))[[1]][1]))
      write.csv(dfFinalCleanRecords,filespace,row.names = FALSE) ##To write the file in csv format in a given path####
      library(sqldf)
      library(RODBCext)
      connHandle <-odbcConnect("ses",uid = "root",pwd = "root")
      for(i in 1:nrow(dfFinalCleanRecords))
      {
        sqlQuery(connHandle,paste("insert into archive.cleandata (Date_time,unitid,noise_value,light_value,temp_value,co2_value,voc_value,humid_value)
                                  values('",dfFinalCleanRecords$Date_time[i],"','",as.character(dfFinalCleanRecords$unitid[i]),"','",dfFinalCleanRecords$noise_value[i],"','",dfFinalCleanRecords$light_value[i],"','",dfFinalCleanRecords$temp_value[i],"','",dfFinalCleanRecords$co2_value[i],"','",dfFinalCleanRecords$voc_value[i],"','",dfFinalCleanRecords$humid_value[i],"')", sep=""))
      }  ###To Write the clean data into the database##
      close(connHandle)
      dfFinalCleanRecords <- NULL  ##To clean the records from the dataframe###
      if(j == nrow(dataf))         
      {  
        send.mail(from = from,
                  to = to,
                  subject="Outlier Process has been completed Successfuly",
                  body = "Hello Team, 
                  Outlier Process has been completed Successfuly and file has been placed in the respective folders",
                  smtp = list(host.name = "smtp.gmail.com", port = 465,user.name="seshan1490@gmail.com",passwd="Varadan@64",ssl=TRUE),
                  authenticate = TRUE,
                  send = TRUE) ##To send an email to ackniwlegde the process outlier process completion###
        Parfile=read.csv("C:/Users/e0013353/Desktop/Parmeter/Aggregation/ParameterFile.csv",header = TRUE) ## To Read the input values from aggregation parameter file ##
        for(i in 1:nrow(Parfile))
        {  ##To Read values columnwise from aggregation parameter file and storing those value in variable##
          Afilename<-Parfile[i,1]  ## To Read the value from first column of paramfile##
          Acolumname<-Parfile[i,2] ## To Read the value from second column of paramfile##
          MiutesToTranform<-Parfile[i,3] ## To Read the value from third column of paramfile##
          id<-Parfile[i,4] ## To Read the value from fourth column of paramfile##
          zone<-Parfile[i,5] ## To Read the value from fifth column of paramfile##
          Level<-Parfile[i,6] ## To Read the value from sixth column of paramfile##
          Stfilename=toString(Afilename) ##To covert the datatype to string ###
          Stcolumname=(Acolumname)      
          MiutoAvg=MiutesToTranform  
          Aggreagatedfile <- Aggregatingfile(Stfilename,Stcolumname,MiutoAvg,id,zone,Level) ##To call the aggregation function###
        }
      }
      }
  }
}



#############################################Function#####################################################
Aggregatingfile<-function(Stfilename,Stcolumname,MiutoAvg,id,zone,Level)  
{
  library(xts) 
  library(zoo)
  library(lubridate)
  file_dict=setwd(cpath) ##To set the working directory ##
  Afile=read.csv(Stfilename,header= TRUE,stringsAsFactors=FALSE)  ##To read the input file###
  file=na.omit(Afile) ##To exclude NA value from file and storing the data in file###
  file_xts=xts(file,order.by=as.POSIXct(file[,Stcolumname],format="%Y-%m-%d %H:%M")) ###To Convert datetime format to standard format####
  file_xts$Date_time=NULL ###To remove the date_time value with old format###
  ep=endpoints(file_xts,on="minutes",k=MiutoAvg) ##To Create the break points##
  Aggregate=period.apply(file_xts,INDEX=ep,FUN=mean)  ##To aggregate the data to user defined interval###
  df=as.data.frame(Aggregate)   ##To covert aggregate data to data frame structure###                
  Date_time=index(Aggregate)    ##To extract the index value from aggregate data frame###
  Date_time=as.character(as.POSIXct(Date_time)) ##To convert to standard datetime format##
  dfDate_time=as.data.frame(Date_time)  ##To convert date time column into a separate dataframe##
  Combinefile=cbind(df,dfDate_time) ###To merge the data time column with the aggregate data##
  Combinefile$unitid.x<-id  ## To replace unid.x value with id value ###
  Combinefile$Days<-wday(Combinefile$Date_time, label = TRUE) ##To derive the weekdays column from datatime column###
  Combinefile$Weekday<-NA ###To create a weekday/weekend column####
  Combinefile$Weekday=as.factor(ifelse(Combinefile$Days=='Mon'| Combinefile$Days=='Tue'| Combinefile$Days=='Wed'| Combinefile$Days=='Thu'| Combinefile$Days=='Fri',"Weekday","Weekend"))
  Combinefile$Start_flag<-NA  ###To Create the Start flag column##
  Combinefile$End_flag<-NA    ###To Create the End flag column##
  Combinefile$Zone<-zone      ##To Create zone column###
  Combinefile$Level<-Level    ##To Create Level column###
  Max_value=max(Combinefile$light_value) ###To find the max light value###
  Min_value=min(Combinefile$light_value) ###To find the min light value###
  Avg_min_max=(Max_value+Min_value)      ###To find the avg of min and max light value##
  setpoint=( Avg_min_max/2)              ### To derive the set point for spliting day into working day/ Non-working day##
  for(i in 1:(length(Combinefile$light_value)-1))
  {
    tail(Combinefile)
    Start_time<-(Combinefile$light_value[i] < setpoint & Combinefile$light_value[i+1] > setpoint)### To derive the start time of office##
    Combinefile$Start_flag[i+1]<-Start_time 
    End_time<-(Combinefile$light_value[i] > setpoint & Combinefile$light_value[i+1] < setpoint) ###To  derive the end time of office###
    Combinefile$End_flag[i+1]<-End_time
    Combinefile$Date_time<-as.character(Combinefile$Date_time) ##To convert the datetime format###
    Start_End_Time<-subset(Combinefile,Combinefile$Start_flag ==TRUE | Combinefile$End_flag ==TRUE,select=1:ncol(Combinefile)) ###To extract the value that falls under office working hours###
    Split_Record<- subset(Combinefile,Combinefile$Date_time >= (Start_End_Time[1,8]) & Combinefile$Date_time <= (Start_End_Time[2,8])) 
    Combinefile$Hours[Combinefile$Date_time %in% Split_Record$Date_time] <- "OFFICEHOURS" ###To create a column hours and tag a value as working and non working hours###
    if(i==(length(Combinefile$light_value)-1))
    {
      colnames(Combinefile)[1]<-"unitid" ###To create a new column with unitid###
      Combinefile$Start_flag<-NULL       ###To remove the start_flag column####
      Combinefile$End_flag<-NULL          ###To remove the End_flag column####
      if(MiutoAvg==5)
      {
        library(sqldf)
        library(RODBCext)
        connHandle <-odbcConnect("ses",uid = "root",pwd = "root")
        for(i in 1:nrow(Combinefile))
        {
          sqlQuery(connHandle,paste("insert into archive.fivemindata (unitid,noise_value,light_value,temp_value,co2_value,voc_value,humid_value,Date_time,Days,Weekday,Zone,Level,Hours)
                                    values('",as.character(Combinefile$unitid[i]),"','",Combinefile$noise_value[i],"','",Combinefile$light_value[i],"','",Combinefile$temp_value[i],"','",Combinefile$co2_value[i],"','",Combinefile$voc_value[i],"','",Combinefile$humid_value[i],"','",Combinefile$Date_time[i],"','",as.character(Combinefile$Days[i]),"','",as.character(Combinefile$Weekday[i]),"','",as.character(Combinefile$Zone[i]),"','",Combinefile$Level[i],"','",Combinefile$Hours[i],"')", sep=""))
        }  ###To push the data into the database###
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
  file=read.csv(Sfilename)            ## To read the file##
  summary(file)                       ## To to get summary of file ##
  UpperOutlierRange=x                 ##Assigning user given upperoutlier range##
  LowerOutlierRange=y                 ##Assigning user given loweroutlier rage##
  file$Flag<-NA                       ##To create a new flag column###
  colname=colnames(file[Scolumname])  ##To get column name ##
  medianValue=median(file[,Scolumname]) ##To get the median value ####
  boxplot(file[,Scolumname], main="BOX PLOT", boxwex=0.1,xlab=colname,ylab="Range") ##To draw a box plot##
  Upper_Outlier <- subset(file, file[,Scolumname] > UpperOutlierRange, select = 1:ncol(file)) ##To find the value greater than upper outlier value###
  lower_Outlier <- subset(file, file[,Scolumname] < LowerOutlierRange, select = 1:ncol(file)) ##To find the value less than lower outlier value##
  AllOutlierRecords <- rbind(lower_Outlier,Upper_Outlier)### To bind the upper and lower outlier records###
  CleanRecords <- subset(file,!(file[,Scolumname] %in% AllOutlierRecords[,Scolumname]),select=c(8,1,Scolumname))##To remove the outlier records from the normal  data##
  CleanRecords$date_time=as.POSIXct(CleanRecords$date_time,format='%Y-%m-%d %H:%M:%S') ## To convert the datatime to standard format###
  mfile1<-zoo(CleanRecords[,-1],CleanRecords[,1]) ###To create a column with value from start time to end time##
  mfile2 <- merge(mfile1,zoo(,seq(start(mfile1),end(mfile1),by="10 secs")), all=TRUE) ###To compare the actual datetime with R created datetime###
  df=as.data.frame(mfile2)    ### To create a dataframe with missing value####                    
  Date_time=index(mfile2)     ###To extract the index value###
  Date_time=as.character(as.POSIXct(Date_time)) ##To convert to standard date time format###
  dfDate_time=as.data.frame(Date_time) ### To store the date time column as data frame###
  Combinefile=cbind(df,dfDate_time) ##To merge the datetime column with the dataframe###
  file$Flag[file[,Scolumname] %in% AllOutlierRecords[,Scolumname]] <- "outlier" ##To tag the values as outlier value###
  return(list(colname,Combinefile,file)) ###To retuen the value####
}

################################################################################################################
Createdirectory<-function(maindirct,fileid)  ###Function to create a directory###
{
  CurrentDate<-Sys.Date()                    ###To assign system date time#####
  folder=paste(fileid,"_",CurrentDate)       ###To Merge sensor id and system datetime####
  filepath=file.path(maindirct,folder)       ###To create a path where the folder has to be created###
  Createfolder=dir.create(filepath,'0777')   ###To Create the folder###
  return(filepath)                           ###To return the value
}
################################################################################
outlierdirectory<-function(Omaindirct,fileid)  
{
  CurrentDate<-Sys.Date()                    ###To assign system date time#####
  folder=paste(fileid,"_",CurrentDate)        ###To Merge sensor id and system datetime####
  filepath=file.path(Omaindirct,folder)      ###To create a path where the folder has to be created###
  Createfolder=dir.create(filepath,'0777')   ###To Create the folder###
  return(filepath)                           ###To return the value
}
###############################################################################
cleandirectory<-function(Cmaindirct)
{
  CurrentDate<-Sys.Date()                     ###To assign system date time#####
  folder=paste("CleanData","_",CurrentDate)   ###To Merge sensor id and system datetime####
  filepath=file.path(Cmaindirct,folder)       ###To create a path where the folder has to be created###
  Createfolder=dir.create(filepath,'0777')    ###To Create the folder###
  return(filepath)                            ###To return the value
}
      }
################################################################################
Aggredirectory<-function(Omaindirct)
{
  CurrentDate<-Sys.Date()                    ###To assign system date time#####
  folder=paste("Files","_",CurrentDate)      ###To Merge sensor id and system datetime####
  filepath=file.path(Omaindirct,folder)      ###To create a path where the folder has to be created###
  Createfolder=dir.create(filepath,'0777')   ###To Create the folder###
  return(filepath)                           ###To return the value
}
}
