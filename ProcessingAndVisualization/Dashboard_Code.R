#########################################################
###############################  RUN Only ONCE  ##########################

# install.packages("funModeling")
# install.packages("plotly")
# install.packages("gridBase")
# install.packages("gridExtra")
# install.packages("ggplot2")
# install.packages("zoo")
# install.packages('reshape2')
# install.packages("lubridate")
# #devtools::install_github("tidyverse/ggplot2")
# #install_github("Ram-N/weatherData")
# install.packages('rsconnect')
# install.packages("DBI")
# install.packages("dplyr")
# install.packages("dbplyr")
# #devtools::install_github("rstudio/pool")
# install.packages("RMySQL")
#
# library(shinydashboard)
# library(funModeling)
# library(ggplot2)
# library(dplyr)
# library(zoo)
# library(shiny)
# library(reshape2)
# library(plotly)
# require(grid)
# library(gridBase)
# library(gridExtra)
# library(lubridate)
# library(plyr)
# library(RODBC)
# library(sqldf)


##  Connecting to Database 

 connHandle <-odbcConnect(dsn="pooja",uid = "root",pwd = "resource@22")
#########################################################

###  USER INTERFACE  ###

ui <- dashboardPage(
  dashboardHeader(title = "Sensor Visualization"),
  dashboardSidebar(
    sidebarMenu(
      
      
      ##  DASHBOARD 1
      menuItem("Building Level ", tabName = "dashboard_1", icon = icon("dashboard")),
      
      ##  DASHBOARD 2
      menuItem( startExpanded = FALSE,"Sensor Level", tabName = "dashboard_2", icon = icon("dashboard"))
    )),
  
  dashboardBody(
    fluidRow( box(title='Input Location',width = 4,selectInput("Location","Location",choices=c("MD11","SOC","RC4","FASS"))) ,
              box(title='Input Time Granularity',width=4,selectInput("AggLevel", "Select the Level", 
                                                                     choices=c("5 Minutes"='fivemindata',"15 Minutes"='fifteenmindata',
                                                                               "30 Minutes"='thirtymindata',"1 Hour"='sixtymindata',"24 Hours"='onedaydata'),
                                                                     selected= "sixtymindata"))),
    
    tabItems(
      tabItem(tabName = "dashboard_1",
              
              fluidRow(
                box(uiOutput("BoxVariabl"),actionButton("goButton4", "Go!"),title = 'Ranges',width=12,collapsible = TRUE,plotOutput("RangePlot")),
                box(title = 'Trends',width=12,collapsible = TRUE,plotlyOutput("CorrPlot_Zones",height='600px'))
                )
      ),
      
      tabItem(tabName = "dashboard_2",
              fluidRow( box(title='Select Sensor',width=4,uiOutput("sensor"),actionButton("goButton1", "Go!"))),
              
              
              fluidRow( box(title='Summary',width=6,collapsible = TRUE,verbatimTextOutput("summary")),
                        box(title = 'Missing Data',width=6, uiOutput("Missing"))),
              
              fluidRow( box(width=2,uiOutput("varx"),uiOutput("vary"),uiOutput("groupVar"),uiOutput('datesCorr'), actionButton("goButton2", "Go!")),
                        box(title='Correlation Plot',width=10,collapsible = TRUE,plotlyOutput("corrPlot")) ),
              
              fluidRow( box(width=6,plotlyOutput("TrenPlot1")),
                        box(width=6,plotlyOutput("TrenPlot2"))  ),
              
              fluidRow( box(uiOutput("var"),width=2,height=650,actionButton("goButton3", "Go!")),
                        box(title = 'Trends',width=10,height=650,collapsible = TRUE,plotlyOutput("trendPlot",height='600px'))),
              
              fluidRow( box(uiOutput("outlierVariable"),actionButton("goButton7", "Go!"),title = 'Outlier Analysis',width=12, plotlyOutput("outlier"),uiOutput("CountSummary"))) 
      )
      
      
    ),
    tags$style(type="text/css",
               ".shiny-output-error { visibility: hidden; }",
               ".shiny-output-error:before { visibility: hidden; }" )
  )  )

###########################################################################################################################
server <- function(input, output) {
  
  ####################  Extracting Data Based on User Input   ################################
  
  fileChoice<-reactive({ req(input$Location,input$AggLevel)
    b<-sqlQuery(connHandle,paste0("SELECT distinct(unitid) from archive.",input$AggLevel," where Building= '",input$Location,"';"))
    b   })
  
  ##  input$Location holds the user input -> MD11/RC4/FASS/SOC
  ##  input$Agglevel holds the user input -> 5 Mins/15 mins/30 mins/1 Hour/24 Hours
  ##  'fileChoice' holds the list of sensors at the location selected by user
  
  output$sensor <- renderUI({
    req(fileChoice)
    print(fileChoice())
    selectInput("dataset", "Select the Sensor", choices=fileChoice())
  })
  
  ##  output$sensor provides the options from fileChoice to user for the sensor selection
  
  ## Get the value of the dataset that is selected by user from the list of datasets
  
  data<-reactive({
    input$goButton1
    isolate({
      req(input$dataset,input$Location,input$AggLevel)
      p<-sqlQuery(connHandle,paste0("SELECT * from archive.",input$AggLevel," where Building= '",input$Location,"' and unitid='",input$dataset,"';"))
      p
    })
    
    ## input$dataset holds the sensor selected by user
    ## data() is a reactive variable and holds the data w.r.t to the choice made by the user and changes based on selection from user
  })
  
  ####################  Summary   ################################
  output$summary <- renderPrint({  req(data()) 
    summary(data()[2:7]) }) 
  
  ####################  Correlation Plot   ################################
  
  
  
  # Pulling the list of variable for choice of variable x - Correlation plot
  output$varx <- renderUI({
    req(data()) 
    selectInput("variablex", "Select the X variable", choices=names(data()[,2:7]))
  })
  
  # Pulling the list of variable for choice of variable y - Correlation plot
  output$vary <- renderUI({
    req(data()) 
    selectInput("variabley", "Select the Y variable", choices=names(data()[,2:7]))
  })
  
  # Group selection for Correlation Plot Checkbox group input
  output$groupVar<-renderUI({
    checkboxGroupInput ("group","Choose Filter",choices=c("Weekday Vs Weekend"='Weekday',"WorkingHours Vs Non-WorkingHours"='Hours',"Zone","Level","Building"))
  })
  
  # Selection of desired dates
  output$datesCorr <- renderUI({
    dates <- data()$date_time
    minval <- min(dates)
    maxval <- max(dates)
    # maxval <- minval + 7890000
    # print(maxval)
    dateRangeInput('InputDateRangeCorr', label = "Choose time-frame:",start = minval, end = maxval, separator = " - ", format = "dd-mm-yyyy")
  })
  
  CorrData<-reactive ({
    input$goButton2
    isolate({
      req(input$variablex,input$variabley)
      start1<-sprintf("%s 00:00:00",min(input$InputDateRangeCorr))
      end1<-sprintf("%s 00:00:00",max(input$InputDateRangeCorr))
      actualData1<-subset(data(), date_time>start1 & date_time<end1)
      week<-actualData1
      vec<-input$group
      week$Day<-NA
      if(length(vec)==1)
      {
        week$Day<-week[,vec[1]]
      }else if(length(vec)==2)
      {
        week$Day<-paste(week[,vec[1]],week[,vec[2]])
      } else if(length(vec)==3)
      {
        week$Day<-paste(week[,vec[1]],week[,vec[2]],week[,vec[3]])
      } else {week$Day<-paste(week[,vec[1]],week[,vec[2]],week[,vec[3]],week[,vec[4]])}
      week
    })
  })
  ## input$InputDateRangeCorr holds the Date Range selected by user. The input is in the form dd/mm/YYYY and dataset contains date_time as dd/mm/YYYY HH:MM:SS
  ## sprintf function concatenates 00:00:00 to the date selected by user to be able to match with the date_time in dataset.
  ## vec contains a list of all the options selected by user
  ## Day is a new dynamic column created for correlation plot, contains the combinations of the options selected by user
  ## This function is for filtering data based on user input for correlation plot
  
  
  output$corrPlot <- renderPlotly({
    week0<-CorrData()
    p<- ggplot(week0,aes_string(x=input$variablex, y=input$variabley, color='Day')) +geom_point() +labs(color = "Category") +theme_classic() +ylab(input$variabley) +xlab(input$variablex) 
  }) 
  
  ## week0 holds the data based on the selection by user as created in the reactive function CorrData() and is used for plotting
  
  # Trendplot for X variable selected for correlation plot
  output$TrenPlot1 <- renderPlotly({
    week1<-CorrData()
    q1<-week1 %>% group_by(Day) %>% plot_ly(x=~date_time, y=as.formula(paste0("~",input$variablex)),color=~Day) %>%
      add_lines(name=~Day)
  })
  
  # Trendplot for Y variable selected for correlation plot
  output$TrenPlot2 <- renderPlotly({
    week2<-CorrData()
    q2<-week2 %>% group_by(Day) %>% plot_ly(x=~date_time, y=as.formula(paste0("~",input$variabley)),color=~Day) %>%
      add_lines(name=~Day)
  })
  
  ####################  Trend Plot ######################
  
  # Trendvar<- checkbox input from user for line graph
  output$var <- renderUI({
    req(data())
    checkboxGroupInput ("Trendvar", "Choose variables",choices=names(data()[,2:7]))
  })
  
  # Line Plot
  output$trendPlot<- renderPlotly({
    input$goButton3
    isolate({
      if(is.null(input$Trendvar)){return()}
     # print(input$Trendvar)
      actualData<-data()
     # View(actualData)
      plots <- lapply(input$Trendvar, function(var) {
        plot_ly(actualData, x = ~date_time, y = as.formula(paste0("~", var))) %>%
          add_lines(name = var)%>%
          add_lines(y=as.formula(paste0("~mean(", var,")")), mode = "lines",showlegend = FALSE,color="black") %>%
          # add_lines(y=as.formula(paste0("~max(",var,")")),mode = "lines",showlegend=FALSE,color="red") %>%
          add_lines(y=as.formula(paste0("~min(",var,")")),mode = "lines",showlegend=FALSE,color="pink")
      })
      plotly:: subplot(plots, nrows = length(plots), shareX = TRUE, titleX = FALSE)
    })
    
  })
  
  ####################  Missing data Summary   ################################
  output$Missing <-renderTable ({
    
    input$goButton1
    isolate({
      req(input$Location,input$dataset)
      path<-sprintf("E:/Internship/Data Analysis/%s/MergeFile/%s.csv",input$Location,input$dataset)
      MissingData<-read.csv(path,header = TRUE)
      temp<-df_status(MissingData)
      b<-data.frame(temp$variable,temp$p_na)
      colnames(b)<-c("Parameter","Missing Percent")
      return(b)
    })
  })
  
  ####################  Outlier Analysis   ######################
  
  outlierChoices<-c("Temperature"='temp_value',"Humidity"='humid_value',"Co2"='co2_value',"VOC"='voc_value',"Light"='light_value',"Noise"='noise_value')
  
  output$outlierVariable <- renderUI({
    selectInput("variableOut", "Select the variable", choices=c("Temperature"='temp_value',"Humidity"='humid_value',"Co2"='co2_value',"VOC"='voc_value',"Light"='light_value',"Noise"='noise_value'))
  })
  
  dataFile<-reactive({
    input$goButton7
    isolate({
      h<- names(outlierChoices[outlierChoices==input$variableOut])
       dataToPlot<-sqlQuery(connHandle,paste0("SELECT date_time,",h, " from archive.",input$variableOut," where unitid= '",input$dataset,"';",sep=""))
      dataToPlot
    })
  })

  
  output$outlier<-renderPlotly({
    
    #location, time, sensor chosen from beginning, only parameter will be chosen on this tab. Each plot is stored in different file
    
    v<-input$variableOut
    h<- names(outlierChoices[outlierChoices==input$variableOut])
     # file<-sqlQuery(connHandle,paste0("SELECT date_time,",h, " from archive.",input$variableOut," where unitid= '",input$dataset,"';",sep=""))
    file<-dataFile()
   # View(file)
   
    plot_ly(file, x = ~date_time, y = as.formula(paste0("~",h)), type = 'scatter', mode = "markers") %>% layout(title=input$dataset)
  })
  
  output$CountSummary <-renderTable ({
    
    req(input$variableOut,dataFile())
    h<- names(outlierChoices[outlierChoices==input$variableOut])
    count<-length(dataFile()[,2])
    a<-sprintf("Number of outliers for %s is % d ",h,count)
    return(a)
  })
  
  
  
  ####################  Dashboard 1   ###########################################################
  
  
  ####################  Range Plot   ################################
  
  # VarBox<- Variable for boxplots for all selected sensors1
  output$BoxVariabl <- renderUI({ 
    selectInput("VarBox", "Select Parameter", choices=c('Temperature','Humidity','Co2','VOC','Noise','Light'))
  })
  
  output$RangePlot <- renderPlot({
    input$goButton4
    isolate({
      combined_data<-sqlQuery(connHandle,paste0("SELECT unitid,",input$VarBox," from archive.",input$AggLevel," where Building= '",input$Location,"';"))
      ggplot(data=combined_data, aes(x=unitid,y=get(input$VarBox))) +geom_boxplot()  +theme_classic() + labs(y=input$VarBox,x=NULL)
    })
  })
  ####################  Trend Plot2   ################################
  output$CorrPlot_Zones<- renderPlotly({
    input$goButton4
    isolate({
      combined_data1<-sqlQuery(connHandle,paste0("SELECT date_time,unitid,",input$VarBox," from archive.",input$AggLevel," where Building= '",input$Location,"';"))
      r<-combined_data1 %>% group_by(unitid) %>% plot_ly(x=~date_time, y=as.formula(paste0("~",input$VarBox)),color=~unitid) %>%
        add_lines(name=~unitid)
    })
  })
  
 
}

shinyApp(ui, server)