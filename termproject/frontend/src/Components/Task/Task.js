import React, { useEffect, useState } from "react";
import { Typography, Button, ColorPicker } from "antd";
import { motion } from "framer-motion";

import "./Task.css";

//Task JSON
// {
//   title:
//   description:
//   task_id:
//   seq_no:
//   lane_id:
//   color:
// }

export default function Task({ task, editTask, deleteTask }) {
  const recentColors = useState([]);
  //if (task.color === undefined) task.color = "#f5deb3 ";
  useEffect(() => {
    console.log("Task changed:", task);
    if (task.color == null || task.color === "") {
      editTask({ ...task, color: "#f5deb3" });
    }
    console.log("Task changed:", task);
  }, [task]);
  const editTaskTitle = (title) => {
    //onsole.log("Editing task title", title);
    if (title === "") return;
    editTask({ ...task, task_title: title });
  };

  const editTaskDescription = (description) => {
    if (description === "") return;
    editTask({ ...task, description: description });
  };

  return (
    <motion.div className="task" style={{ backgroundColor: task.color }}>
      <div className="task-text">
        <div
          style={{
            display: "flex",
            alignItems: "center",
            justifyContent: "space-between",
          }}
        >
          <Typography.Title
            level={3}
            editable={{
              tooltip: "click to edit text",
              triggerType: "text",
              onChange: editTaskTitle,
            }}
          >
            {task.task_title}
          </Typography.Title>
          <ColorPicker
            color={task.color}
            onChange={(color) => {
              console.log("Changing color to", color.toHexString());
              editTask({ ...task, color: color.toHexString() });
            }}
            presets={[
              {
                label: "Recommended",
                colors: [
                  "#F5222D4D",
                  "#FA8C164D",
                  "#FADB144D",
                  "#8BBB114D",
                  "#52C41A4D",
                  "#13A8A84D",
                  "#1677FF4D",
                  "#2F54EB4D",
                  "#722ED14D",
                  "#EB2F964D",
                ],
              },
            ]}
          >
            <Button ghost>&nbsp;</Button>
          </ColorPicker>
        </div>

        <Typography.Paragraph
          editable={{
            tooltip: "click to edit text",
            triggerType: "text",
            onChange: editTaskDescription,
          }}
        >
          {task.description}
        </Typography.Paragraph>
      </div>
    </motion.div>
  );
}
