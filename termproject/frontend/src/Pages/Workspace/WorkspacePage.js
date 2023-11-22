import React, { useState, useEffect } from "react";
import Board from "../../Components/Board/Board";
import PageLayout from "../../Components/PageLayout/PageLayout";
import { useParams, useNavigate } from "react-router-dom";
import { customFetch } from "../../utils/fetchUtils";
import { Layout, Typography, Menu, ConfigProvider, theme, Button } from "antd";
import { useUser } from "../SignInPage";

export default function Workspace() {
  //TEMPORARY UNTIL CAN BE GRABBED FROM BACKEND
  const current_board_id = useParams().current_board_id;
  const [collapsed, setCollapsed] = useState(false);
  const navigate = useNavigate();
  const { Sider, Content } = Layout;
  const user = useUser();
  const [boards, setBoards] = useState([]);

  //Get all boards for user
  useEffect(() => {
    if(!user) return;
    console.log(user.user_id, [current_board_id]);
    fetch(`http://localhost:3300/api/boards?user_id=${user.user_id}`)
      .then((res) => res.json())
      .then((data) => {
        if (current_board_id === undefined) {
          if (data.length === 0) {
            createBoard();
            window.location.reload();
          } else {
            console.log("Moving to:", `/workspace/${data[0].board_id}`);
             //navigate to first board
            navigate(`/workspace/${data[0].board_id}`);

          }
        } else {  
          console.log("Setting boards:", data);
          setBoards(data);
        }
        console.log(data);
      });
  }, [user, current_board_id]);

  const updateBoard = (board) => {
    setBoards(
      boards.map((b) => {
        customFetch(
          `http://localhost:3300/api/boards/${board.board_id}`,
          {
            title: board.title,
          },
          "PUT"
        );
        if (b.board_id === board.board_id) {
          return board;
        } else {
          return b;
        }
      })
    );
  };

  const createBoard = () => {
    customFetch(
      `http://localhost:3300/api/boards`,
      {
        title: "New Board",
        user_id: user.user_id,
        team_name: "New Team",
      },
      "POST"
    ).then((data) => {
      console.log("created board", data[0]);
      setBoards([...boards, data[0]]);
    });
  };

  const deleteBoard = (board_id) => {
    customFetch(
      `http://localhost:3300/api/boards/${board_id}`,
      {},
      "DELETE"
    ).then((data) => {
      console.log(data);
      setBoards(boards.filter((board) => board.board_id !== board_id));
      navigate(`/workspace`);
    });
  };

  return (
    <PageLayout signInRequired>
      {/*Sider*/}
      <Layout style={{ height: "100%" }}>
        <ConfigProvider theme={{ algorithm: theme.darkAlgorithm }}>
          <Sider
            style={{ height: "100%" }}
            collapsible
            collapsed={collapsed}
            onCollapse={(value) => setCollapsed(value)}
          >
            <Typography.Title level={5} style={{ paddingLeft: "1rem" }}>
              Boards
            </Typography.Title>
            <Menu
              theme="dark"
              mode="inline"
              defaultSelectedKeys={[current_board_id]}
              style={{ height: "100%", borderRight: 0 }}
            >
              {boards.map((board) => (
                <Menu.Item
                  key={board.board_id}
                  label={board.title}
                  onClick={() => {
                    navigate(`/workspace/${board.board_id}`);
                  }}
                >
                  <div
                    style={{
                      display: "flex",
                      justifyContent: "space-between",
                      alignItems: "center",
                    }}
                  >
                    <Typography.Text
                      editable={{
                        tooltip: "Click to edit board name",
                        triggerType: ["icon"],
                        onChange: (value) => {
                          updateBoard({ ...board, title: value });
                        },
                      }}
                    >
                      {board.title}
                    </Typography.Text>
                    <div
                      onClick={() => {
                        deleteBoard(board.board_id);
                      }}
                    >
                      X
                    </div>
                  </div>
                </Menu.Item>
              ))}
              <Button
                type="primary"
                onClick={() => {
                  createBoard();
                }}
              >
                Add Board
              </Button>
            </Menu>
            {/* Add Board Button */}
          </Sider>
        </ConfigProvider>
        <Content>
          {/* Board Generation */}
          {current_board_id === null ? (
            boards.length > 0 ? (
              <Board board_id={boards[0].board_id} collapsed={collapsed} />
            ) : (
              boards.length === 0 && <h1>No boards found</h1>
            )
          ) : (
            <Board board_id={current_board_id} collapsed={collapsed} />
          )}
        </Content>
      </Layout>
    </PageLayout>
  );
}
