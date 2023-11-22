import React from 'react'
import Board from '../Components/Board/Board';
import { ConfigProvider, theme } from "antd";
import NavBar from "../Components/NavBar/NavBar";
import PageLayout from '../Components/PageLayout/PageLayout';


export default function TaskTestPage() {
  return (
    <PageLayout>
      <Board />
    </PageLayout>
  )
}
