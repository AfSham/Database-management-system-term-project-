import "./App.css";
import React, { useState, useEffect } from "react";
import LaneContainer from "./Components/LaneContainer/LaneContainer";
import { ConfigProvider, theme } from "antd";



// DEPRECATED!!! DO NOT USE!!! (Use Index.js and add to router instead)
// ----------------------------------------------------------------------------------------------------
// ----------------------------------------------------------------------------------------------------
// ----------------------------------------------------------------------------------------------------
// ----------------------------------------------------------------------------------------------------
export default function App() {
  const [users, setUsers] = useState([]);

  //Deletes a user from the database
  function deleteUser(userID) {
    console.log("Deleting user with ID: " + userID);
    fetch("http://localhost:3300/api/users/" + userID, {
      method: "DELETE",
    })
      .then(() => {
        console.log("Deleted user with ID: " + userID);
        getUsers(setUsers);
      })
      .catch((error) => {
        console.log(error);
      });
  }

  //Adds a random user to the database
  function addRandomUser() {
    fetch("http://localhost:3300/api/users", {
      method: "POST",
      body: JSON.stringify({
        username: "random" + Math.random(),
        email: "" + Math.random() + "@gmail.com",
        password: "random" + Math.random(),
      }),
      headers: {
        "Content-type": "application/json; charset=UTF-8",
      },
    }).then(() => {
      getUsers(setUsers);
    });
  }

  //Fetches users from backend
  function getUsers(func) {
    fetch("http://localhost:3300/api/users")
      .then((response) => response.json())
      .then((data) => {
        func(data);
      });
  }
  //Runs once on page load
  // useEffect(() => {
  //   getUsers(setUsers);
  // }, []);

  return (
    <div className="App">
      <button
        onClick={() => {
          getUsers(setUsers);
        }}
        className="button"
      >
        Refresh User List
      </button>
      <button
        onClick={() => {
          addRandomUser();
        }}
        className="button"
      >
        Add random user
      </button>
      <h2>Users List:</h2>
      <div className="user-container">
        {users.map((user) => (
          <UserTag key={user.UserID} user={user} deleteUser={deleteUser} />
        ))}
      </div>
    </div>
  );
}


const UserTag = ({ user, deleteUser }) => {
  return (
    <div className="user-tag">
      <h1>{user.Username}</h1>
      <h1>{user.Email}</h1>
      <button
        onClick={() => {
          deleteUser(user.UserID);
        }}
        className="button"
      >
        Delete
      </button>
    </div>
  );
};
