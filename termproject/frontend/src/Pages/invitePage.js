import React, { useEffect } from "react";
import PageLayout from "../Components/PageLayout/PageLayout";
import { useParams } from "react-router-dom";
import { customFetch } from "../utils/fetchUtils";
import { useUser } from "./SignInPage";

export default function InvitePage() {
  const { team_id } = useParams();
  const user = useUser();
  console.log(team_id);

  useEffect(() => {
    if (!user) {
      window.location.href = "http://localhost:3000/signin";
      return;
    }
    if (!team_id) {
      window.location.href = "http://localhost:3000/workspace";
      return;
    }
    customFetch(
      `http://localhost:3300/api/onATeam`,
      {
        user_id: user.user_id,
        team_id: team_id,
      },
      "POST"
    ).then(() => {
      window.location.href = "http://localhost:3000/workspace";
    });
  }, [team_id, user]);

  return (
    <PageLayout signInRequired>
      <div>
        <h1>Invite Page</h1>
      </div>
    </PageLayout>
  );
}
