## **DS 5110: Big Data Systems | Final Project**
## **State of Virginia Traffic Reliability | MAP21**

### by Christian Schroeder (dbn5eu), Timothy Tyree (twt6xy), Colin Warner (ynq9ya)

## **Brief Overview**

**Project Description:** This study develops a target setting methodology for the (Moving Ahead for Progress) MAP-21 Interstate Travel Time Reliability Measure of “Percent of the person-miles traveled on the Interstate that are Reliable” (PMTR-IS). The study uses Virginia specific data for a set of independent variables (Number of Lanes, Terrain, Urban Designation, Equivalent Property Damage Only Rate, Lane Impacting Incident Rate, Truck Percentage, Presence of Safety Service Patrol, Hourly Volume, and Volume/Capacity Ratio) to predict if a MAP-21 reporting segment is reliable. This is used to estimate Predicted PMTR-IS with the MAP-21 specified formula. This is an ongoing project at the Virginia Department of Transportation (VDOT) and they have asked our team to explore more-advanced classification models. They previously went through 1,563 different configurations of Classification and Regression Tree (CART) models. VDOT provided us with all of the raw data - 12 csv files that need extensive preprocessing before a final dataset can analyzed. The data consists of the metrics listed above for each year between 2017 and 2020, for each highway segment in Virginia. Furthermore, VDOT forecasted their metrics out to 2024. Our goal is to use the actual data up to 2020 to find an accurate model using train, test, and validation splits. If that model is found, we can use the forecasted metrics to classify *future* unreliable segments.

**What is MAP-21?** "MAP-21, the Moving Ahead for Progress in the 21st Century Act (P.L. 112-141), was signed into law by President Obama on July 6, 2012. Funding surface transportation programs at over \\$105 billion for fiscal years (FY) 2013 and 2014, MAP-21 is the first long-term highway authorization enacted since 2005. MAP-21 is a milestone for the U.S. economy and the Nation’s surface transportation program. By transforming the policy and programmatic framework for investments to guide the system’s growth and development, MAP-21 creates a streamlined and performance-based surface transportation program and builds on many of the highway, transit, bike, and pedestrian programs and policies established in 1991." Source: https://www.fhwa.dot.gov/map21/


**Notebook Description:** In this particular Notebook, we will walk through (1) the importing of our data from the Virginia Department of Transportation, and (2) the necessary preprocessing of our data to ensure our data is in a format that is reliable for Splitting, Exploratory Analysis, and Modeling. To avoid lines of repetitive code (reading, joining, etc.) we wrote a custom preprocessing class. The following section imports said class and explains how it works.

---
