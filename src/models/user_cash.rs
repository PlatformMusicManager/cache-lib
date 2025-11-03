pub struct PlaylistUserCash {
    id: i64,
    title: String,
    creator: String
}


pub struct UserCash {
    username: String,
    playlists: Vec<PlaylistUserCash>
}
