This is the term project for CSCI 4370/6370 DBMS, Summer 2023
Group Members: Razavan Beldeanu, Afsaneh Shams, Isabella Humphrey, and Chris Evans


Before you can do this, you must install node.js and make sure it is added to your PATH (Google how to do this if confused)
Heres the link to node.js: https://nodejs.org/en


## How to spin up the front end

In a terminal window, cd to the frontend directory and run "npm i" to install all the packages then run "npm start"
It will by default start a web-server that serves to localhost:3000.
To access the page just open your web browser of choice and visit localhost:3000


## How to spin up the backend

Make sure you have a postgres server set up, if you need help look at the docs from the ToolTalk we did. Make sure the postgres password is 1234
Once you have postgres installed and setup create a database called 4370DB (We can change this later, this is just how it is set up in the code)

Keep the frontend terminal running, and In a seperate terminal cd to the backend directory and run "npm i" to install all the packages then run "npm start"


With both of these terminals running you will have a backend and frontend all running locally on your machine! (If you make changes to the backend, you will have to stop the backend server from running by using CTRL+C in that terminal, then re-run it) 



