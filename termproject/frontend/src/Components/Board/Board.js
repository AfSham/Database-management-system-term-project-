import React, { useEffect, useState } from "react";
import LaneContainer from "../LaneContainer/LaneContainer";
import { Layout, Typography, Menu, Button, Modal, Descriptions } from "antd";

const { Header, Content, Footer } = Layout;

export default function Board({ board_id, collapsed }) {
  const [modalOpen, setModalOpen] = useState(false); //team modal visibility
  const [board, setBoard] = useState();
  // const [board, setBoard] = useState({
  //   board_id: 1,
  //   name: "Test Board",
  // }); //board object

  useEffect(() => {
    if(board_id === undefined) return;
    fetch(`http://localhost:3300/api/boards/${board_id}`)
      .then((res) => res.json())
      .then((data) => {
        console.log(data[0]);
        setBoard(data[0]);
      });
  }, [board_id]);

  return (
    <>
      <Button
        type="primary"
        style={{ margin: "1rem", marginBottom: "-2.4rem" }}
        onClick={() => setModalOpen(true)}
      >
        View Team
      </Button>
      {board_id === undefined ? null : (
        <LaneContainer board_id={board_id} collapsed={collapsed} />
      )}
      <Modal
        open={modalOpen}
        onCancel={() => setModalOpen(false)}
        footer={[
          <Button type="primary" onClick={() => setModalOpen(false)}>
            Ok
          </Button>,
        ]}
      >
        <TeamModalContent team_id={board?.team_id} />
      </Modal>
    </>
  );
}

const TeamModalContent = ({ team_id }) => {
  const [teamMembers, setTeamMembers] = useState([
    {
      name: "Test User",
      user_id: 1,
    },
    {
      name: "Test User 2",
      user_id: 2,
    },
  ]); //team members

  useEffect(() => {
    fetch(`http://localhost:3300/api/onateam?team_id=${team_id}`)
      .then((res) => res.json())
      .then((data) => {
        console.log("ONATEAM:", data);
        setTeamMembers(data);
      });
  }, [team_id]);

  const removeMember = (id) => {
    fetch(`http://localhost:3300/api/onateam?user_id=${id}&team_id=${team_id}`, {
      method: "DELETE",
    })
      .then((res) => res.json())
      .then((data) => {
        console.log(data);
      });
    setTeamMembers(teamMembers.filter((member) => member.user_id !== id));
    window.location.reload();
  };

  return (
    <>
      <Descriptions column={1} title="Team Members" bordered>
        {teamMembers.map((member) => (
          <Descriptions.Item label={member.name}>
            <div style={{ display: "flex", justifyContent: "space-evenly" }}>
              <Typography.Text>{member.role}</Typography.Text>
              {teamMembers.length < 2 ? null : (
                <Button
                  type="primary"
                  danger
                  onClick={() => {
                    removeMember(member.user_id);
                  }}
                >
                  Remove
                </Button>
              )}
            </div>
          </Descriptions.Item>
        ))}
      </Descriptions>
      <br />
      <Typography.Title level={5}>
        Share Link: <a href={`http://localhost:3000/invite/${team_id}`}>{`http://localhost:3000/invite/${team_id}`}</a>{" "}
      </Typography.Title>
    </>
  );
};
