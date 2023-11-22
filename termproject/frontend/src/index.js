import React from "react";
import ReactDOM from "react-dom/client";
import "./index.css";
import reportWebVitals from "./reportWebVitals";
import { createBrowserRouter, RouterProvider } from "react-router-dom";
import TaskTestPage from "./Pages/TaskTestPage";
import ErrorPage from "./Pages/ErrorPage";
import HomePage from "./Pages/HomePage";
import SignInPage from "./Pages/SignInPage";
import SignUpPage from "./Pages/SignUpPage";
import Workspace from "./Pages/Workspace/WorkspacePage";
import InvitePage from "./Pages/invitePage";

const router = createBrowserRouter([
  {
    path: "/taskTest",
    element: (
      <TaskTestPage />
    ),
  },
  {
    path: "/",
    element: (
      <HomePage />
    ),
  },
  {
    path: "/signin",
    element: (
      <SignInPage />
    ),
  },
  {
    path: "/signup",
    element: (
      <SignUpPage />
    ),
  },
  {
    path: "*",
    element: (
      <ErrorPage />
    ),
  },
  {
    path: "/workspace/:current_board_id",
    element: (
      <Workspace />
    ),
  },
  {
    path: "/workspace",
    element: (
      <Workspace />
    ),
  },
  {
    path: "/invite/:team_id",
    element: (
      <InvitePage />
    ),
  }

]);

const root = ReactDOM.createRoot(document.getElementById("root"));
root.render(<RouterProvider router={router} />);

// If you want to start measuring performance in your app, pass a function
// to log results (for example: reportWebVitals(console.log))
// or send to an analytics endpoint. Learn more: https://bit.ly/CRA-vitals
//reportWebVitals(console.log);
