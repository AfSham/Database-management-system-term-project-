import React, { useEffect, useState } from "react";
import { Skeleton, Typography, Button } from "antd";
import { calcLength, motion, Reorder, AnimatePresence } from "framer-motion";
import "./Lane.css";
import Task from "../Task/Task";

//Lane JSON
// {
//   lane_id:
//   name:
//   seq_no:
//   board_id:
//   tasks: task[]
// }
export default function Lane({
  lane,
  deleteLane,
  highlight = false,
  whileTaskDragging,
  onDragEnd,
  editLane,
}) {
  //const [tasks, setTasks] = useState(inLane.tasks); // holds all the tasks for this lane // holds the lane object

  const editLaneName = (name) => {
    if (name === "") return;
    editLane({ ...lane, lane_name: name });
  };

  const calcSeqNo = (values) => {
    let seq_no = 1;
    values.forEach((value) => {
      value.seq_no = seq_no;
      seq_no++;
    });

    return values;
  };

  const setTasks = (tasks) => {
    editLane({
      ...lane,
      //Maybe will double up? idk how this is gonna work lmao
      tasks: [...tasks],
    });
  };

  const addTask = () => {
    let newTask = {
      task_title: "New Task",
      description: "Enter description",
      seq_no: lane.tasks.length + 1,
      lane_id: lane.lane_id,
      color: "#f5deb3",
    };
    setTasks([...lane.tasks, newTask]);
  };

  const editTask = (task) => {
    //console.log("editing task:", task);
    setTasks(lane.tasks.map((t) => (t.task_id === task.task_id ? task : t)));
  };

  const removeTask = (task_id) => {
    setTasks(lane.tasks.filter((task) => task.task_id !== task_id));
  };



  return lane ? (
    //Lane has loaded
    <div className={highlight ? "lane drag-hover" : "lane"}>
      <div className="lane-header">
        <Typography.Title
          level={3}
          editable={{
            tooltip: "click to edit text",
            triggerType: "text",
            onChange: editLaneName,
          }}
        >
          {lane.lane_name}
        </Typography.Title>
        <Button
          type="danger"
          onClick={() => {
            deleteLane(lane.lane_id);
          }}
        >
          Delete
        </Button>
      </div>

      {/* Render all the tasks */}
      {lane.tasks?.length > 0 ? (
      <Reorder.Group
        as="div"
        axis="y"
        values={lane.tasks}
        onReorder={(tasks) => {
          // console.log("reorder");
          setTasks(tasks)
        }}
        className="tasks"
      >
        <AnimatePresence>

        {lane.tasks.map((task) => (
          <Reorder.Item
            as="div"
            drag
            onDrag={(event)=> {
              whileTaskDragging(event);
              // console.log("dragging", lane);
            }}
            onDragEnd={(event) => {
              onDragEnd(event, task, lane);
              // console.log("drag end", lane);
            }}
            key={task.task_id}
            exit={{ opacity: 0, height: 0 }}
            initial={{ opacity: 0, height: 0 }}
            animate={{ opacity: 1, height: "auto", transition: { duration: 0.1 } }}

            value={task}
          >
            <Task task={task} editTask={editTask} deleteTask={removeTask}/>
          </Reorder.Item>
        ))}
        </AnimatePresence>

      </Reorder.Group>
      ) : (
        <div className="no-tasks">
          <Typography.Text type="secondary">No tasks</Typography.Text>
        </div>
      )}
      {/* Add task button */}
      <Button
        type="dashed"
        // ghost
        block
        onClick={() => {
          addTask();
        }}
      >
        Add task
      </Button>
    </div>
  ) : (
    //Lane has not loaded
    <div>
      <Skeleton active />
    </div>
  );
}
