// const faker = require('@faker-js/faker');

// const express = require('express');
// const pg = require('pg');
//const bodyParser = require('body-parser');
import express from "express";
import pg from "pg";
import bodyParser from "body-parser";
import { faker } from "@faker-js/faker";
import cors from "cors";
import e from "express";
import bcrypt from "bcryptjs";
const salt = bcrypt.genSaltSync(10)

const app = express();
const port = 3300;
const pool = new pg.Pool({
  user: "postgres",
  host: "localhost",
  database: "4370DB",
  password: "1234",
  port: 5432,
});

app.use(express.json());

app.use(bodyParser.json());

app.use(cors());
app.listen(port, () => {
  console.log(`Example app listening on port ${port}`);
});

// HELPER FUNCTIONS------------------------------------------------------------
// USE THESE!!!!! THEY WILL GREATLY SIMPLIFY YOUR CODE look at USERS ROUTES at the bottom to see how much easier it is to use these
// MAINLY LOOK AT poolQuery() and poolQueryWithResponse() as it will write whatever query you give it to the database and catch any errors

//Adds an object to the specified table
//The object must have the same keys as the table
// example:
// const obj = {
//    lane_name:
//    board_id:
//    seq_no :
// }
// table = "Lanes"
async function addObjectToTable(table, obj) {
  return new Promise((resolve, reject) => {
    const listOfKeys = Object.keys(obj);
    const listOfValues = Object.values(obj);
    const query =
      `INSERT INTO "${table}" ` +
      `("${listOfKeys.join('", "')}")` +
      " VALUES " +
      `('${listOfValues.join("', '")}') RETURNING *`;

    resolve(poolQuery(query));
  });
}

async function updateObejctInTable(table, obj, idKey) {
  return new Promise((resolve, reject) => {
    const listOfKeys = Object.keys(obj);
    const listOfValues = Object.values(obj);
    const query =
      `UPDATE "${table}" ` +
      `SET ("${listOfKeys.join('", "')}") = ` +
      `('${listOfValues.join("', '")}') ` +
      `WHERE ${idKey} = ${obj[idKey]} RETURNING *`;

    console.log(query);
    resolve(poolQuery(query));
  });
}

async function deleteTaskByLaneId(lane_id) {
  return new Promise((resolve, reject) => {
    const query = `DELETE FROM "Tasks" WHERE lane_id = ${lane_id}`;
    resolve(poolQuery(query));
  });
}

async function deleteLaneByBoardId(board_id) {
  //Get all lanes for board
  //Delete all tasks for each lane
  //Delete all lanes for board

  const lanes = await poolQuery(`SELECT * FROM "Lanes" WHERE board_id = ${board_id}`);
  lanes.forEach((lane) => {
    deleteTaskByLaneId(lane.lane_id);
  });
  return new Promise((resolve, reject) => {
    const query = `DELETE FROM "Lanes" WHERE board_id = ${board_id}`;
    resolve(poolQuery(query));
  });
}

//Takes in a query and returns a promise that resolves to the result of the query
// In short if you want to get the result from this function you need to use .then() or await
async function poolQuery(query) {
  return new Promise((resolve, reject) => {
    console.log(query);
    pool.query(query, (error, result) => {
      if (error) {
        console.log("FAILED");
        //throw error;
        reject(error);
      } else {
        resolve(result.rows);
      }
    });
  });
}

//Same as poolQuery() but also takes in a response object and sends the result of the query to the client for you
async function poolQueryWithResponse(query, response) {
  return new Promise((resolve, reject) => {
    poolQuery(query)
      .catch((error) => {
        response.status(400).json(error);
        reject(error);
      })
      .then((result) => {
        response.status(200).json(result);
        resolve(result);
      });
  });
}

async function getAllFromTable(table) {
  return await poolQuery(`SELECT * FROM "${table}"`);
}

// GENERATE RANDOM DATA--------------------------------------------------------

// Function to generate random data for the Users table
function generateRandomUserData() {
  const username = faker.person.firstName();
  const email = faker.internet.email();
  const password = faker.internet.password();
  return {
    name: username,
    email: email,
    password: password,
  };
}

// Function to generate random data for the Teams table
function generateRandomTeamData() {
  return {
    team_name: faker.word.noun(),
  };
}

// Function to generate random data for the Boards table
async function generateRandomBoardData() {
  let listOfTeams = await getAllFromTable("Teams");
  listOfTeams = listOfTeams.map((team) => team.team_id);

  return {
    title: faker.lorem.word(),
    team_id: listOfTeams[Math.floor(Math.random() * listOfTeams.length)],
  };
}

// Function to generate random data for the OnATeam table
async function generateRandomOnATeamData() {
  let teams = await getAllFromTable("Teams");
  teams = teams.map((team) => team.team_id);
  let users = await getAllFromTable("Users");
  users = users.map((user) => user.user_id);
  return {
    user_id: users[Math.floor(Math.random() * users.length)],
    team_id: teams[Math.floor(Math.random() * teams.length)],
  };
}

// Function to generate random data for the Lanes table
async function generateRandomLaneData() {
  let boards = await getAllFromTable("Boards");
  boards = boards.map((board) => board.board_id);
  return {
    lane_name: faker.lorem.word(),
    board_id: boards[Math.floor(Math.random() * boards.length)],
    seq_no: faker.number.int({ min: 1, max: 100 }),
  };
}

// Function to generate random data for the Tasks table
async function generateRandomTaskData() {
  let lanes = await getAllFromTable("Lanes");
  lanes = lanes.map((lane) => lane.lane_id);
  return {
    task_title: faker.lorem.word(),
    lane_id: lanes[Math.floor(Math.random() * lanes.length)],
    seq_no: faker.number.int({ min: 1, max: 100 }),
    description: faker.lorem.sentence(),
    color: null, // Add the "color" attribute with a value of null
  };
}

// ROUTES-----------------------------------------------------------------------
// -----------------------------------------------------------------------------
// -----------------------------------------------------------------------------
// -----------------------------------------------------------------------------

// Random Fill Routes-----------------------------------------------------------
// Route to fill the Users table with random data
app.post("/api/users/fill/:numFill", (request, response) => {
  console.log("FILLING USERS TABLE");
  const numUsersToAdd = request.params.numFill;
  for (let i = 0; i < numUsersToAdd; i++) {
    addObjectToTable("Users", generateRandomUserData());
  }
  response
    .status(201)
    .send(`Successfully added ${numUsersToAdd} users to the table.`);
});

// Route to fill the Teams table with random data
app.post("/api/teams/fill/:numFill", (request, response) => {
  console.log("FILLING TEAMS TABLE");
  const numTeamsToAdd = request.params.numFill;
  for (let i = 0; i < numTeamsToAdd; i++) {
    addObjectToTable("Teams", generateRandomTeamData());
  }
  response
    .status(201)
    .send(`Successfully added ${numTeamsToAdd} teams to the table.`);
});

// Route to fill the Boards table with random data
app.post("/api/boards/fill/:numFill", async (request, response) => {
  console.log("FILLING BOARDS TABLE");
  const numBoardsToAdd = request.params.numFill;
  for (let i = 0; i < numBoardsToAdd; i++) {
    addObjectToTable("Boards", await generateRandomBoardData());
  }
  response
    .status(201)
    .send(`Successfully added ${numBoardsToAdd} boards to the table.`);
});

// Route to fill the OnATeam table with random data
app.post("/api/onateam/fill/:numFill", async (request, response) => {
  console.log("FILLING ONATEAM TABLE");
  const numOnATeamToAdd = request.params.numFill;
  for (let i = 0; i < numOnATeamToAdd; i++) {
    addObjectToTable("OnATeam", await generateRandomOnATeamData());
  }
  response
    .status(201)
    .send(`Successfully added ${numOnATeamToAdd} onATeam to the table.`);
});

// Route to fill the Lanes table with random data
app.post("/api/lanes/fill/:numFill", async (request, response) => {
  console.log("FILLING LANES TABLE");
  const numLanesToAdd = request.params.numFill;
  for (let i = 0; i < numLanesToAdd; i++) {
    addObjectToTable("Lanes", await generateRandomLaneData());
  }
  response
    .status(201)
    .send(`Successfully added ${numLanesToAdd} lanes to the table.`);
});

// Route to fill the Tasks table with random data
app.post("/api/tasks/fill/:numFill", async (request, response) => {
  console.log("FILLING TASKS TABLE");
  const numTasksToAdd = request.params.numFill;
  for (let i = 0; i < numTasksToAdd; i++) {
    addObjectToTable("Tasks", await generateRandomTaskData());
  }
  response
    .status(201)
    .send(`Successfully added ${numTasksToAdd} tasks to the table.`);
});

// USER Routes -----------------------------------------------------------

// GET route for the Users table
app.get("/api/users", async (request, response) => {
  console.log("GOT REQUEST");
  console.log("GETTING USERS");
  if (!request.query.email && !request.query.password) {
    pool.query(
      'SELECT * FROM "Users" ORDER BY "user_id" ASC ',
      (error, result) => {
        if (error) {
          console.log("FAILED");
          response.status(500).json({ error });
        } else {
          console.log("SUCCESS RETURNING USERS");
          response.status(200).json(result.rows);
        }
      }
    );
  } else {
    const hashedPassword = await bcrypt.hashSync(request.query.password, salt)
    poolQueryWithResponse(
      `SELECT * FROM "Users" WHERE "email" = '${request.query.email}' and "password" = '${hashedPassword}'`,
      response
    );
  }
});

app.get("/api/users/:id", (request, response) => {
  console.log("GOT REQUEST");
  console.log("GETTING USER");
  const id = parseInt(request.params.id);
  poolQueryWithResponse(
    `SELECT * FROM "Users" WHERE "user_id" = ${id}`,
    response
  );
});

app.post("/api/users", async (request, response) => {
  console.log("GOT REQUEST");
  console.log("ADDING USER");
  const { name, email, password } = request.body;
  const hashedPassword = await bcrypt.hash(password, salt)
  const resp = await addObjectToTable("Users", {
    name,
    email,
    password: hashedPassword,
  });
  response.status(201).send(resp);
});

app.delete("/api/users/:id", async (request, response) => {
  console.log("GOT REQUEST");
  console.log("DELETING USER");
  const id = parseInt(request.params.id);
  poolQueryWithResponse(
    `DELETE FROM "Users" WHERE "user_id" = ${id}`,
    response
  );
});

app.delete("/api/users/:id", (request, response) => {
  console.log("GOT REQUEST");
  console.log("DELETING USER");
  const id = parseInt(request.params.id);
  pool.query(
    'DELETE FROM "Users" WHERE "UserID" = $1',
    [id],
    (error, result) => {
      if (error) {
        console.log("FAILED");
        response.status(500).json({ error });
      } else {
        console.log("SUCCESS DELETING USER");
        response.status(200).send(`User deleted with ID: ${id}`);
      }
    }
  );
});

// TEAM Routes -----------------------------------------------------------

async function createTeam(team_name) {
  console.log("ADDING TEAM");
  const resp = await addObjectToTable("Teams", {
    team_name,
  });
  return resp;
}
// }
// GET route for the Teams table
app.get("/api/teams", async (request, response) => {
  try {
    const teams = await poolQuery(`SELECT * FROM "Teams"`);
    response.status(200).json(teams);
  } catch (error) {
    response.status(500).json({ error: "Internal server error" });
  }
});

app.get("/api/teams/:id", (request, response) => {
  console.log("GOT REQUEST");
  console.log("GETTING TEAM");
  const id = parseInt(request.params.id);
  poolQueryWithResponse(
    `SELECT * FROM "Teams" WHERE "team_id" = ${id}`,
    response
  );
});

app.post("/api/teams", async (request, response) => {
  console.log("GOT REQUEST");
  const { team_name } = request.body;
  const resp = await createTeam(team_name);
  //Check if resp is an error
  if (resp.error) {
    response.status(500).json(resp);
  }
  response.status(201).send(resp);
});

app.delete("/api/teams/:id", async (request, response) => {
  console.log("GOT REQUEST");
  console.log("DELETING TEAM");
  const id = parseInt(request.params.id);
  poolQueryWithResponse(
    `DELETE FROM "Teams" WHERE "team_id" = ${id}`,
    response
  );
});

//BOARD Routes -----------------------------------------------------------`
// GET route for the Boards table
app.get("/api/boards", async (request, response) => {
  if (!request.query.user_id) {
    try {
      const boards = await poolQuery(`SELECT * FROM "Boards"`);
      response.status(200).json(boards);
    } catch (error) {
      response.status(500).json({ error: "Internal server error" });
    }
  } else if (request.query.user_id) {
    try {
      const boards = await poolQuery(
        `SELECT "board_id", "title"  FROM "Boards" b inner join "OnATeam" o on b."team_id" = o."team_id" where "user_id" = ${request.query.user_id}`
      );
      response.status(200).json(boards);
    } catch (error) {
      response.status(500).json({ error: "Internal server error" });
    }
  }
});

app.get("/api/boards/:id", (request, response) => {
  console.log("GOT REQUEST");
  console.log("GETTING BOARD");
  const id = parseInt(request.params.id);
  poolQueryWithResponse(
    `SELECT * FROM "Boards" WHERE "board_id" = ${id}`,
    response
  );
});

app.post("/api/boards", async (request, response) => {
  console.log("GOT REQUEST");
  //Create a new team for the board
  createTeam(request.body.team_name).then((team) => {
    //Create the board
    const { title, user_id } = request.body;
    console.log("ADDING BOARD", title, team[0].team_id);
    //Add the user to the team
    addObjectToTable("OnATeam", {
      user_id,
      team_id: team[0].team_id,
    });
    //Add the board to the database
    const resp = addObjectToTable("Boards", {
      title,
      team_id: team[0].team_id,
    })
      .then((resp) => {
        response.status(201).send(resp);
      })
      .catch((err) => {
        response.status(500).json(err);
      });
  });
});

app.delete("/api/boards/:id", async (request, response) => {
  console.log("GOT REQUEST");
  console.log("DELETING BOARD");
  const id = parseInt(request.params.id);
  //delete all the lanes 
  deleteLaneByBoardId(id);
  poolQueryWithResponse(
    `DELETE FROM "Boards" WHERE "board_id" = ${id}`,
    response
  );
});

app.put("/api/boards/:id", async (request, response) => {
  console.log("GOT REQUEST");
  console.log("UPDATING BOARD");
  const id = parseInt(request.params.id);
  const { title } = request.body;
  updateObejctInTable(
    "Boards",
    { title: title, board_id: id },
    "board_id"
  ).then((resp) => {
    response.status(200).send(resp);
  });
});

//LANE Routes -----------------------------------------------------------`
// GET route for the Lanes table
app.get("/api/lanes", async (request, response) => {
  console.log("GOT REQUEST");
  if (!request.query.board_id) {
    try {
      const lanes = await poolQuery(`SELECT * FROM "Lanes"`);
      response.status(200).json(lanes);
    } catch (error) {
      response.status(500).json({ error: "Internal server error" });
    }
  } else {
    console.log("GETTING LANES: ", request.query);
    //Get all lanes then add an array of tasks to each lane
    poolQuery(
      `SELECT * FROM "Lanes" WHERE "board_id" = ${request.query.board_id} ORDER BY "seq_no" ASC`
    ).then((lanes) => {
      if (lanes.length === 0) response.status(200).json(lanes);
      lanes.forEach((lane) => {
        console.log("GETTING TASKS for lane", lane.lane_id);
        poolQuery(
          `SELECT * FROM "Tasks" WHERE "lane_id" = ${lane.lane_id} ORDER BY "seq_no" ASC`
        ).then((tasks) => {
          lane.tasks = tasks;
          if (lane === lanes[lanes.length - 1]) {
            response.status(200).json(lanes);
          }
        });
      });
    });
  }
});

app.get("/api/lanes/:id", (request, response) => {
  console.log("GOT REQUEST");
  console.log("GETTING LANE");
  const id = parseInt(request.params.id);
  poolQueryWithResponse(
    `SELECT * FROM "Lanes" WHERE "lane_id" = ${id}`,
    response
  );
});

app.post("/api/lanes", async (request, response) => {
  console.log("GOT REQUEST");
  const { lane_name, board_id, seq_no } = request.body;
  console.log("ADDING LANE", lane_name, board_id);
  const resp = await addObjectToTable("Lanes", {
    lane_name,
    board_id,
    seq_no,
  });
  response.status(201).send(resp);
});

app.delete("/api/lanes/:id", async (request, response) => {
  console.log("GOT REQUEST");
  console.log("DELETING LANE");
  const id = parseInt(request.params.id);
  //Delete all tasks in the lane
  deleteTaskByLaneId(id);
  poolQueryWithResponse(
    `DELETE FROM "Lanes" WHERE "lane_id" = ${id}`,
    response
  );
});

app.put("/api/lanes/:id", async (request, response) => {
  console.log("GOT REQUEST");
  console.log("UPDATING LANE");
  const id = parseInt(request.params.id);
  const { lane_name, seq_no, tasks } = request.body;
  let returnObject;
  updateObejctInTable(
    "Lanes",
    { lane_name: lane_name, seq_no: seq_no, lane_id: id },
    "lane_id"
  )
    .then(async (resp) => {
      returnObject = resp[0];
      //Loop through all tasks and update them
      returnObject.tasks = [];
      
      for (let i = 0; i < tasks.length; i++) {
        const task = tasks[i];
        if (task.task_id === undefined) {
          //Add task to database
          const resp = await addObjectToTable("Tasks", {
            task_title: task.task_title,
            lane_id: id,
            description: task.description,
            seq_no: task.seq_no,
            color: task.color,
          });
          returnObject.tasks = [...returnObject.tasks, resp[0]];
        } else {
          await updateObejctInTable(
            "Tasks",
            {
              task_title: task.task_title,
              description: task.description,
              seq_no: task.seq_no,
              task_id: task.task_id,
              color: task.color,
            },
            "task_id"
          );
          returnObject.tasks = [...returnObject.tasks, task];
        }
      }
      console.log("RETURNING OBJECT", returnObject);
      response.status(200).send(returnObject);




      // tasks.forEach(async (task) => {
      //   //check if task is new
      //   if (task.task_id === undefined) {
      //     //Add task to database
      //     addObjectToTable("Tasks", {
      //       task_title: task.task_title,
      //       lane_id: id,
      //       description: task.description,
      //       seq_no: task.seq_no,
      //     }).then((resp) => {
      //       returnObject.tasks = [...returnObject.tasks, resp];
      //       console.log("ADDED TASK", resp);
      //     });
      //   } else {
      //     await updateObejctInTable(
      //       "Tasks",
      //       {
      //         task_title: task.task_title,
      //         description: task.description,
      //         seq_no: task.seq_no,
      //         task_id: task.task_id,
      //       },
      //       "task_id");
      //     returnObject.tasks = [...returnObject.tasks, task];
      //   }
      // });
    });
});

//TASK Routes -----------------------------------------------------------
// GET route for the Tasks table
app.get("/api/tasks", async (request, response) => {
  if (!request.query.lane_id) {
    try {
      const tasks = await poolQuery(`SELECT * FROM "Tasks"`);
      response.status(200).json(tasks);
    } catch (error) {
      response.status(500).json({ error: "Internal server error" });
    }
  } else {
    poolQueryWithResponse(
      `SELECT * FROM "Tasks" WHERE "lane_id" = ${request.query.lane_id} ORDER BY "seq_no" ASC`,
      response
    );
  }
});

app.get("/api/tasks/:id", (request, response) => {
  console.log("GOT REQUEST");
  console.log("GETTING TASK");
  const id = parseInt(request.params.id);
  poolQueryWithResponse(
    `SELECT * FROM "Tasks" WHERE "task_id" = ${id}`,
    response
  );
});

app.post("/api/tasks", async (request, response) => {
  console.log("GOT REQUEST");
  const { task_title, lane_id, description, seq_no } = request.body;
  console.log("ADDING TASK", task_title, lane_id);
  const resp = await addObjectToTable("Tasks", {
    task_title,
    lane_id,
    description,
    seq_no,
  });
  response.status(201).send(resp);
});

app.delete("/api/tasks/:id", async (request, response) => {
  console.log("GOT REQUEST");
  console.log("DELETING TASK");
  const id = parseInt(request.params.id);
  poolQueryWithResponse(
    `DELETE FROM "Tasks" WHERE "task_id" = ${id}`,
    response
  );
});

app.put("/api/tasks/:id", async (request, response) => {
  console.log("GOT REQUEST");
  console.log("UPDATING TASK");
  const id = parseInt(request.params.id);
  const { task_title, description, seq_no, lane_id } = request.body;
  updateObejctInTable(
    "Tasks",
    {
      task_title: task_title,
      description: description,
      seq_no: seq_no,
      task_id: id,
      lane_id: lane_id,
    },
    "task_id"
  ).then((resp) => {
    response.status(200).send(resp);
  });
});

// GET route for the OnATeam table
app.get("/api/onateam", async (request, response) => {
  if (request.query.user_id) {
    //Get all teams for a user
    poolQueryWithResponse(
      `SELECT * FROM "OnATeam" WHERE "user_id" = ${request.query.user_id}`,
      response
    );
  } else if (request.query.team_id) {
    //Get all users for a team
    const returnObject = [];
    const uids = await poolQuery(`SELECT * FROM "OnATeam" WHERE "team_id" = ${request.query.team_id}`);
    for (let i = 0; i < uids.length; i++) {
      const user = await poolQuery(`SELECT * FROM "Users" WHERE "user_id" = ${uids[i].user_id}`);
      returnObject.push(user[0]);
    }
    response.status(200).send(returnObject);

  } else {
    //Get all users on all teams
    poolQueryWithResponse(`SELECT * FROM "OnATeam"`, response);
  }
});

app.get("/api/onateam/:id", (request, response) => {
  console.log("GOT REQUEST");
  console.log("GETTING ONATEAM");
  const id = parseInt(request.params.id);
  poolQueryWithResponse(
    `SELECT * FROM "OnATeam" WHERE "on_id" = ${id}`,
    response
  );
});

app.post("/api/onateam", async (request, response) => {
  console.log("GOT REQUEST");
  const { user_id, team_id } = request.body;
  console.log("ADDING ONATEAM", user_id, team_id);
  const resp = await addObjectToTable("OnATeam", {
    user_id,
    team_id,
  });
  response.status(201).send(resp);
});

app.delete("/api/onateam", async (request, response) => {
  console.log("GOT REQUEST");
  console.log("DELETING ONATEAM");
  const { user_id, team_id } = request.query;
  poolQueryWithResponse(
    `DELETE FROM "OnATeam" WHERE "user_id" = ${user_id} AND "team_id" = ${team_id}`,
    response
  );

});
