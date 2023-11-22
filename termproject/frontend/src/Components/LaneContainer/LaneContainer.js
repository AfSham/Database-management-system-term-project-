import React, { useState, useEffect } from "react";
import Lane from "../Lane/Lane";
import "./LaneContainer.css";
import { Button } from "antd";
import { Reorder, AnimatePresence } from "framer-motion";
import { customFetch } from "../../utils/fetchUtils";

//Takes a boardId ot get all the lanes for that board
export default function LaneContainer({ board_id, collapsed = false }) {
  const [lanes, setLanes] = useState([]);
  const [highlightLane, setHighlightLane] = useState(null); //lane that is being hovered over

  const deleteLane = (lane_id) => {
    //Delete Lane from DB
    customFetch(`http://localhost:3300/api/lanes/${lane_id}`, {}, "DELETE").then(
      (data) => {
        console.log("Deleted Lane:", data);
        //Delete Lane from state
        setLanes(lanes.filter((lane) => lane.lane_id !== lane_id));
      }
    );
  };
  const addLane = () => {

    //Add Lane to DB
    customFetch("http://localhost:3300/api/lanes", {
      board_id: board_id,
      lane_name: "New Lane",
      seq_no: lanes.length + 1,
    }, "POST").then((data) => {
      console.log("Added Lane:", data);
      setLanes([...lanes, {...data[0], tasks: []}]);
    });
  };

  const whileTaskDragging = (event) => {
    //Calculate what lane the task is being dragged over
    let xVal = event.pageX;
    collapsed ? (xVal -= 80) : (xVal -= 200);
    let laneNum = Math.floor(xVal / 250);
    setHighlightLane(laneNum);
  };

  const editLane = (lane) => {
    //Update lane in DB
    console.log("Editing lane:", lane);
    customFetch(`http://localhost:3300/api/lanes/${lane.lane_id}`, {
      lane_id: lane.lane_id,
      lane_name: lane.lane_name,
      seq_no: lane.seq_no,
      tasks: lane.tasks,
    }, "PUT").then((data) => {
      console.log("Lane Returned from db", data);
      const result = lanes.map((l) => (l.lane_id === lane.lane_id ? data : l));
      console.log("Resulting new Lane :", result);
      setLanes(result);
    });
  };

  const calcSeqNo = (values) => {
    let seq_no = 1;
    values.forEach((value) => {
      value.seq_no = seq_no;
      seq_no++;
    });
  };

  //Fetches all the lanes for a board
  useEffect(() => {
    fetch(`http://localhost:3300/api/lanes?board_id=${board_id}`)
      .then((res) => res.json())
      .then((data) => {
        console.log("Got Lanes:", data);
        //sort lanes by seq_no
        data.sort((a, b) => a.seq_no - b.seq_no);
        //sort tasks by seq_no
        data.forEach((lane) => {
          lane.tasks?.sort((a, b) => a.seq_no - b.seq_no);
        });

        setLanes(data);
        });
  }, [board_id]);

  //Controlls the highlighting and task lane change
  const taskDragEnded = (result, task, oldLane) => {
    let newLane = lanes[highlightLane];
    console.log("BEFORE CALC", oldLane.tasks);
    calcSeqNo(oldLane.tasks);
    console.log("AFTER CALC", oldLane.tasks);

    if (newLane === oldLane) {
      //Task was dragged back to original lane
      //Update DB with new seq_no
      editLane(oldLane);
      setHighlightLane(null);
      return;
    }
    if (newLane === undefined) {
      //Task was dragged outside of lanes
      //Delete task

      //update DB
      customFetch(`http://localhost:3300/api/tasks/${task.task_id}`, {}, "DELETE").then((data) => {
        console.log("Task Returned from db", data);
      });

      setLanes(
        lanes.map((lane) => {
          if (lane.lane_id === oldLane.lane_id) {
            return {
              ...lane,
              tasks: lane.tasks.filter((t) => t.task_id !== task.task_id),
            };
          } else {
            return lane;
          }
        })
      );
      setHighlightLane(null);
      return;
    }
    calcSeqNo(newLane.tasks);
    //Create new task with new lane id and seq_no
    const newTask = {
      ...task,
      lane_id: newLane.lane_id,
      seq_no: newLane.tasks.length + 1,
    };
    //Update lanes to reflect lane change for a task

    //update DB
    customFetch(`http://localhost:3300/api/tasks/${task.task_id}`, {
      task_id: task.task_id,
      task_title: task.task_title,
      description: task.description,
      seq_no: newTask.seq_no,
      lane_id: newTask.lane_id,
      color: task.color,
    }, "PUT").then((data) => {
      console.log("Task Returned from db", data);
    });

    //update state
    setLanes(
      lanes.map((lane) => {
        if (lane.lane_id === newLane.lane_id) {
          return {
            ...lane,
            tasks: [...lane.tasks, newTask],
          };
        } else if (lane.lane_id === oldLane.lane_id) {
          return {
            ...lane,
            tasks: lane.tasks.filter((t) => t.task_id !== task.task_id),
          };
        } else {
          return lane;
        }
      })
    );
    // calcSeqNo(oldLane.tasks);
    // calcSeqNo(newLane.tasks);
    //Remove highlight
    setHighlightLane(null);
  };

  const setLanesWithNewSeqNo = (values) => {

    let seq_no = 1;
    values.forEach((value) => {
      value.seq_no = seq_no;
      seq_no++;
    });

    console.log("New Lanes with seq no:", values);
    //Update DB with new seq_no
    values.forEach((lane) => {
      customFetch(`http://localhost:3300/api/lanes/${lane.lane_id}`, {
        lane_id: lane.lane_id,
        lane_name: lane.lane_name,
        seq_no: lane.seq_no,
        tasks: lane.tasks,
      }, "PUT").then((data) => {
        console.log("Lane Returned from db", data);
      });
    });
    setLanes(values);
  };

  return (
    <Reorder.Group
      as="div"
      axis="x"
      values={lanes}
      onReorder={setLanesWithNewSeqNo}
      className="lane-container"
    >
      <AnimatePresence>
        {lanes.map((lane, index) => (
          <Reorder.Item
            as="div"
            key={lane.lane_id}
            value={lane}
            exit={{ opacity: 0, width: 0 }}
            transition={{ duration: 0.2 }}
            initial={{ opacity: 0, width: 0, overflow: "hidden" }}
            animate={{ opacity: 1, width: 250, overflow: "visible" }}
          >
            <Lane
              lane={lane}
              deleteLane={deleteLane}
              whileTaskDragging={whileTaskDragging}
              onDragEnd={taskDragEnded}
              highlight={index === highlightLane}
              editLane={editLane}
            />
          </Reorder.Item>
        ))}
      </AnimatePresence>
      {/* Button to add lane */}
      <Button
        type="primary"
        onClick={() => {
          addLane();
        }}
      >
        Add lane
      </Button>
    </Reorder.Group>
  );
}
