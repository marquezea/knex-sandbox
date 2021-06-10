const knex = require('knex')({
    client: 'mysql',
    connection: {
      host : 'dbclubgame.cluster-crwp0vykwmea.us-west-2.rds.amazonaws.com',
      user : 'ksuynwmxyu',
      password : 'U4DRv8MtPdfitKoVqC',
      database : 'clubgame'
    }
  });
  
  function rowMapper(row) {
      if (row == null) {
          return null;
      }
      const obj = {};
      const cols = ['tenantid', 'q', 'password'];
      // if (adtCols != null) cols.push(...adtCols);
      Object.keys(row)
          .filter(attr => !cols.includes(attr))
          .forEach(attr => obj[attr] = row[attr]);
      return obj;
  }
  
  //const colsBingoGame = ['bingo_game_uid','fullname'];
  //const colsRounds = ['seq'];
  //const colsWinners = ['ball_sequence','customer_uid','game_level'];
  const colsBingoGame = ['bingo_game_uid','event_date','price','fullname'];
  const colsRounds = ['seq','prize_quad','prize_line','prize_ine2','prize_3lines','prize_bingo','prize_bingo2','prize_accum','max_balls','total_prize'];
  const colsWinners = ['game_round_seq','game_level','ball_sequence','card_number','customer_uid','prizeamount'];
  
  async function testKnex()  {
    try {
  
      // Query both of the rows.
      const selectedRows = await knex('bingo_games')
        .select('bingo_games.uid as bingo_game_uid', 'bingo_games.event_date', 'bingo_games.price', 'bingo_games.fullname', 
          'game_rounds.seq', 'game_rounds.prize_quad', 'game_rounds.prize_line', 'game_rounds.prize_line2', 'game_rounds.prize_3lines', 'game_rounds.prize_bingo', 'game_rounds.prize_bingo2', 'game_rounds.prize_accum', 'game_rounds.max_balls', 'game_rounds.total_prize', 
          'bingo_winners.game_round_seq', 'bingo_winners.game_level', 'bingo_winners.ball_sequence', 'bingo_winners.card_number', 'bingo_winners.customer_uid', 'bingo_winners.prizeamount')
        .join('series', 'bingo_games.serie_uid', '=', 'series.uid')
        .join('game_rounds', 'bingo_games.uid', '=', 'game_rounds.bingo_game_uid')
        .join('bingo_winners', 'bingo_games.uid', '=', 'bingo_winners.bingo_game_uid')
        .where(knex.raw('bingo_games.tenantid = "123456"'))
        .andWhere(knex.raw('series.tenantid = "123456"'))
        .andWhere(knex.raw('game_rounds.tenantid = "123456"'))
        .andWhere(knex.raw('game_rounds.seq = bingo_winners.game_round_seq'))
        .andWhere(knex.raw('bingo_winners.tenantid = "123456"'))
        .andWhere(knex.raw('bingo_games.status = "completed"'))
        .andWhere(knex.raw('curdate()-date(bingo_games.event_date) between 0 and 3'))
        .andWhere(knex.raw('bingo_games.uid in (select distinct bingo_game_uid from orders x where x.customer_uid = "ec570f1b-9920-4fb9-abf2-0420b8a05793")'))
        .orderBy(knex.raw('bingo_games.event_date, game_rounds.seq, bingo_winners.ball_sequence, bingo_winners.card_number')).toSQL();
  
      
      function createObj(row, columns) {
        var obj = {};
        Object.keys(row)
        .filter(attr => columns.includes(attr))
        .forEach(attr => obj[attr] = row[attr]);
        return obj;
      }
  
      // map over the results
      var finalResult = []; // final JSON result
      let bingoGameObject = {}; // single bingo Game object with array of rounds inside
      let gameRoundObject = {}; // single round with array of winners inside
      let lastBingoGame = '';
      let lastGameRound = 0;
      for (var i=0; i<selectedRows.length; i++) {
        let row = selectedRows[i];
  
        if ((lastGameRound !== row['seq'])) {
          if (gameRoundObject['seq'] && gameRoundObject['winners'].length > 0) {
            bingoGameObject['rounds'].push(gameRoundObject);
          }
          gameRoundObject = createObj(row, colsRounds);
          gameRoundObject['winners'] = [];
  
        }
  
        if ((lastBingoGame !== row['bingo_game_uid'])) {
          if (bingoGameObject['bingo_game_uid'] && bingoGameObject['rounds'].length > 0) {
            finalResult.push(bingoGameObj);
          } 
  
          bingoGameObject = createObj(row, colsBingoGame);
          bingoGameObject['rounds'] = [];
        } 
  
        let winnerObject = createObj(row,colsWinners);
        gameRoundObject['winners'].push(winnerObject);
  
        lastBingoGame = bingoGameObject['bingo_game_uid'];
        lastGameRound = gameRoundObject['seq'];
  
      }
      if (bingoGameObject['rounds'].length > 0) {
        if (gameRoundObject['winners'].length > 0) {
          bingoGameObject['rounds'].push(gameRoundObject);
        }
        finalResult.push(bingoGameObject);
      } 
  
      console.log(finalResult);
    
      // Finally, add a catch statement
    } catch(e) {
      console.error(e);
    };
  }
  
  testKnex();
  